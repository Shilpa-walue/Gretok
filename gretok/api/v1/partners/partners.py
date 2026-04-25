import frappe
from frappe import _

from gretok.schemas.v1.partners.partners import MANDATORY_FIELDS, OPTIONAL_FIELDS, ALLOWED_VALUES
from gretok.utils.validator import validate_payload
from gretok.utils.response import success_response, error_response, not_found_response, conflict_response
from gretok.utils.logger import log_info, log_error

LOG_TITLE = "Partners API"

# ── FETCH ALL PROJECTS BY PARTNER ─────────────────────────────────────────────

@frappe.whitelist()
def get_partner_projects(**kwargs):
	"""
	Fetch all projects (Solar Farm + BESS) belonging to a partner.

	Endpoint: GET /api/method/gretok.api.v1.partners.partners.get_partner_projects?partner=PRT-0001

	Query Params:
		partner (str): Mandatory - Partner ID
		project_type (str): Optional - "Solar Farm" or "BESS"
		limit (int): Default 20
		offset (int): Default 0
	"""
	kwargs.pop("cmd", None)

	log_info(LOG_TITLE, "Get Partner Projects Request", kwargs)

	partner = kwargs.get("partner")
	if not partner:
		return error_response(_("partner is mandatory"), http_status_code=400)

	if not frappe.db.exists("Partners", partner):
		return not_found_response(_("Partner '{0}' does not exist").format(partner))

	limit = min(int(kwargs.get("limit") or 20), 100)
	offset = int(kwargs.get("offset") or 0)
	project_type = kwargs.get("project_type")

	solar_farm_projects = []
	bess_projects = []

	# Fetch Solar Farm Projects
	if not project_type or project_type == "Solar Farm":
		solar_farm_projects = frappe.get_all(
			"Solar Farm Project",
			filters={"partner": partner},
			fields=[
				"name", "project_name", "solar_farm_type",
				"location_state", "location_district",
				"dc_installed_capacity_mwp", "ac_installed_capacity_mw",
				"commission_date", "crediting_period_start_date",
				"crediting_period_end_date", "grid_connection_type",
				"panel_technology", "creation", "modified",
			],
			order_by="creation desc",
		)
		for p in solar_farm_projects:
			p["project_type"] = "Solar Farm"

	# Fetch BESS Projects
	if not project_type or project_type == "BESS":
		bess_projects = frappe.get_all(
			"BESS Project",
			filters={"partner": partner},
			fields=[
				"name", "project_name", "bess_configuration",
				"battery_technology", "rated_energy_capacity_kwh",
				"rated_power_output_kw", "location_state", "location_district",
				"commission_date", "crediting_period_start_date",
				"crediting_period_end_date", "bess_operating_mode",
				"creation", "modified",
			],
			order_by="creation desc",
		)
		for p in bess_projects:
			p["project_type"] = "BESS"

	# Merge and sort by creation desc
	all_projects = solar_farm_projects + bess_projects
	all_projects.sort(key=lambda x: str(x.get("creation") or ""), reverse=True)

	# Apply pagination after merge
	paginated = all_projects[offset: offset + limit]

	total_solar = len(solar_farm_projects)
	total_bess = len(bess_projects)
	total = total_solar + total_bess

	response = success_response(
		_("Projects fetched successfully for partner '{0}'").format(partner),
		data={
			"partner": partner,
			"projects": paginated,
			"total": total,
			"total_solar_farm": total_solar,
			"total_bess": total_bess,
			"limit": limit,
			"offset": offset,
		},
	)

	log_info(LOG_TITLE, "Get Partner Projects Response", response)

	return response


# ── CREATE ────────────────────────────────────────────────────────────────────

@frappe.whitelist()
def create_partner(**kwargs):
	"""
	Endpoint: POST /api/method/gretok.api.v1.partners.partners.create_partner
	"""
	kwargs.pop("cmd", None)

	log_info(LOG_TITLE, "Incoming Request", kwargs)

	error = validate_payload(kwargs, MANDATORY_FIELDS, ALLOWED_VALUES)
	if error:
		return error

	# Check duplicate email
	if frappe.db.exists("Partners", {"email_id": kwargs.get("email_id")}):
		return conflict_response(
			_("A Partner with email '{0}' already exists").format(kwargs.get("email_id"))
		)

	doc_data = {
		"doctype": "Partners",
		"naming_series": "PRT-.####",
	}

	for field in MANDATORY_FIELDS:
		doc_data[field] = kwargs.get(field)

	for field in OPTIONAL_FIELDS:
		value = kwargs.get(field)
		if value is not None:
			doc_data[field] = value

	try:
		doc = frappe.get_doc(doc_data)
		doc.insert(ignore_permissions=True)
		frappe.db.commit()
	except Exception as e:
		log_error(LOG_TITLE, "Insert Failed", kwargs, exc=e)
		return error_response(_("Failed to create Partner: {0}").format(str(e)), http_status_code=500)

	response_data = _build_partner_response(doc)

	frappe.publish_realtime("partner_created", response_data, after_commit=True)

	response = success_response(
		_("Partner created successfully"),
		data={"partner": response_data},
	)

	log_info(LOG_TITLE, "Response", response)

	return response


# ── FETCH ALL ─────────────────────────────────────────────────────────────────

@frappe.whitelist()
def get_all_partners(**kwargs):
	"""
	Endpoint: GET /api/method/gretok.api.v1.partners.partners.get_all_partners

	Query Params:
		limit (int): Default 20
		offset (int): Default 0
		company_type (str): Filter by company type
	"""
	kwargs.pop("cmd", None)

	limit = min(int(kwargs.get("limit") or 20), 100)
	offset = int(kwargs.get("offset") or 0)

	filters = {}
	if kwargs.get("company_type"):
		filters["company_type"] = kwargs.get("company_type")

	partners = frappe.get_all(
		"Partners",
		filters=filters,
		fields=[
			"name", "organization_name", "email_id",
			"phone_number", "company_type", "asset_category",
			"total_partner_tokens", "carbon_credits",
			"wallet_address", "creation", "modified",
		],
		limit=limit,
		start=offset,
		order_by="creation desc",
	)

	total = frappe.db.count("Partners", filters=filters)

	return success_response(
		_("Partners fetched successfully"),
		data={
			"partners": partners,
			"total": total,
			"limit": limit,
			"offset": offset,
		},
	)


# ── FETCH SINGLE ──────────────────────────────────────────────────────────────

@frappe.whitelist()
def get_partner(**kwargs):
	"""
	Endpoint: GET /api/method/gretok.api.v1.partners.partners.get_partner?name=PRT-0001
	"""
	kwargs.pop("cmd", None)

	name = kwargs.get("name")
	if not name:
		return error_response(_("name is mandatory"), http_status_code=400)

	if not frappe.db.exists("Partners", name):
		return not_found_response(_("Partner '{0}' does not exist").format(name))

	doc = frappe.get_doc("Partners", name)

	return success_response(
		_("Partner fetched successfully"),
		data={"partner": _build_partner_response(doc)},
	)


# ── UPDATE ────────────────────────────────────────────────────────────────────

@frappe.whitelist()
def update_partner(**kwargs):
	"""
	Endpoint: PUT /api/method/gretok.api.v1.partners.partners.update_partner

	Body:
		name (str): Mandatory - Partner ID
		... any fields to update
	"""
	kwargs.pop("cmd", None)

	log_info(LOG_TITLE, "Update Request", kwargs)

	name = kwargs.get("name")
	if not name:
		return error_response(_("name is mandatory"), http_status_code=400)

	if not frappe.db.exists("Partners", name):
		return not_found_response(_("Partner '{0}' does not exist").format(name))

	# Validate company_type if being updated
	if kwargs.get("company_type") and kwargs["company_type"] not in ALLOWED_VALUES["company_type"]:
		return error_response(
			_("company_type must be one of: {0}").format(", ".join(ALLOWED_VALUES["company_type"])),
			http_status_code=400,
		)

	updatable_fields = list(MANDATORY_FIELDS.keys()) + OPTIONAL_FIELDS

	try:
		doc = frappe.get_doc("Partners", name)
		for field in updatable_fields:
			value = kwargs.get(field)
			if value is not None:
				setattr(doc, field, value)
		doc.save(ignore_permissions=True)
		frappe.db.commit()
	except Exception as e:
		log_error(LOG_TITLE, "Update Failed", kwargs, exc=e)
		return error_response(_("Failed to update Partner: {0}").format(str(e)), http_status_code=500)

	response = success_response(
		_("Partner updated successfully"),
		data={"partner": _build_partner_response(doc)},
	)

	log_info(LOG_TITLE, "Update Response", response)

	return response


# ── DELETE ────────────────────────────────────────────────────────────────────

@frappe.whitelist()
def delete_partner(**kwargs):
	"""
	Endpoint: DELETE /api/method/gretok.api.v1.partners.partners.delete_partner

	Body:
		name (str): Mandatory - Partner ID
	"""
	kwargs.pop("cmd", None)

	log_info(LOG_TITLE, "Delete Request", kwargs)

	name = kwargs.get("name")
	if not name:
		return error_response(_("name is mandatory"), http_status_code=400)

	if not frappe.db.exists("Partners", name):
		return not_found_response(_("Partner '{0}' does not exist").format(name))

	try:
		frappe.delete_doc("Partners", name, ignore_permissions=True)
		frappe.db.commit()
	except Exception as e:
		log_error(LOG_TITLE, "Delete Failed", kwargs, exc=e)
		return error_response(_("Failed to delete Partner: {0}").format(str(e)), http_status_code=500)

	response = success_response(
		_("Partner '{0}' deleted successfully").format(name),
		data={"name": name},
	)

	log_info(LOG_TITLE, "Delete Response", response)

	return response


# ── HELPERS ───────────────────────────────────────────────────────────────────

def _build_partner_response(doc):
	return {
		"name": doc.name,
		"organization_name": doc.organization_name,
		"company_legal_name": doc.company_legal_name,
		"company_registration_number": doc.company_registration_number,
		"company_type": doc.company_type,
		"country_of_registration": doc.country_of_registration,
		"state_of_registration": doc.state_of_registration,
		"registered_office_address": doc.registered_office_address,
		"email_id": doc.email_id,
		"phone_number": doc.phone_number,
		"company_gst": doc.company_gst,
		"company_pan": doc.company_pan,
		"asset_category": doc.asset_category,
		"primary_contact_name": doc.primary_contact_name,
		"primary_contact_email": doc.primary_contact_email,
		"primary_contact_phone": doc.primary_contact_phone,
		"total_partner_tokens": doc.total_partner_tokens,
		"carbon_credits": doc.carbon_credits,
		"wallet_address": doc.wallet_address,
		"gtka": doc.gtka,
		"gtk": doc.gtk,
		"gtkcc": doc.gtkcc,
		"gtkgp": doc.gtkgp,
		"total_carbon_saved": doc.total_carbon_saved,
		"creation": str(doc.creation),
		"modified": str(doc.modified),
	}