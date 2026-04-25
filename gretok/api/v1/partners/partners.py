import frappe
from frappe import _

from gretok.schemas.v1.partners.partners import MANDATORY_FIELDS, OPTIONAL_FIELDS, ALLOWED_VALUES
from gretok.utils.validator import validate_payload
from gretok.utils.response import success_response, error_response, not_found_response, conflict_response
from gretok.utils.logger import log_info, log_error

LOG_TITLE = "Partners API"


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