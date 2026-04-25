import frappe
from frappe import _

from gretok.schemas.v1.leads.leads import MANDATORY_FIELDS, OPTIONAL_FIELDS, ALLOWED_VALUES, DEFAULTS
from gretok.utils.validator import validate_payload
from gretok.utils.response import success_response, error_response, not_found_response, conflict_response
from gretok.utils.logger import log_info, log_error

LOG_TITLE = "Leads API"


# ── CREATE ────────────────────────────────────────────────────────────────────

@frappe.whitelist(allow_guest=True)
def create_lead(**kwargs):
	"""
	Endpoint: POST /api/method/gretok.api.v1.leads.leads.create_lead
	"""
	kwargs.pop("cmd", None)

	log_info(LOG_TITLE, "Incoming Request", kwargs)

	error = validate_payload(kwargs, MANDATORY_FIELDS, ALLOWED_VALUES)
	if error:
		return error

	if frappe.db.exists("Leads", {"email_id": kwargs.get("email_id")}):
		return conflict_response(
			_("A Lead with email '{0}' already exists").format(kwargs.get("email_id"))
		)

	doc_data = {"doctype": "Leads"}

	for field in MANDATORY_FIELDS:
		doc_data[field] = kwargs.get(field)

	for field in OPTIONAL_FIELDS:
		value = kwargs.get(field)
		if value is not None:
			doc_data[field] = value

	if not doc_data.get("status"):
		doc_data["status"] = DEFAULTS["status"]

	try:
		doc = frappe.get_doc(doc_data)
		doc.insert(ignore_permissions=True)
		frappe.db.commit()
	except Exception as e:
		log_error(LOG_TITLE, "Insert Failed", kwargs, exc=e)
		return error_response(_("Failed to create Lead: {0}").format(str(e)), http_status_code=500)

	response_data = _build_lead_response(doc)

	frappe.publish_realtime("lead_created", response_data, after_commit=True)

	response = success_response(
		_("Lead created successfully"),
		data={"lead": response_data},
	)

	log_info(LOG_TITLE, "Response", response)

	return response


# ── FETCH ALL ─────────────────────────────────────────────────────────────────

@frappe.whitelist()
def get_all_leads(**kwargs):
	"""
	Endpoint: GET /api/method/gretok.api.v1.leads.leads.get_all_leads

	Query Params:
		limit (int): Default 20
		offset (int): Default 0
		status (str): Contacted / Partnered / Declined
		project_type (str): Solar Farm / BESS
	"""
	kwargs.pop("cmd", None)

	limit = min(int(kwargs.get("limit") or 20), 100)
	offset = int(kwargs.get("offset") or 0)

	filters = {}
	if kwargs.get("status"):
		filters["status"] = kwargs.get("status")
	if kwargs.get("project_type"):
		filters["project_type"] = kwargs.get("project_type")

	leads = frappe.get_all(
		"Leads",
		filters=filters,
		fields=[
			"name", "organization_name", "email_id",
			"phone_number", "status", "project_name",
			"project_type", "asset_category", "creation", "modified",
		],
		limit=limit,
		start=offset,
		order_by="creation desc",
	)

	total = frappe.db.count("Leads", filters=filters)

	return success_response(
		_("Leads fetched successfully"),
		data={
			"leads": leads,
			"total": total,
			"limit": limit,
			"offset": offset,
		},
	)


# ── FETCH SINGLE ──────────────────────────────────────────────────────────────

@frappe.whitelist()
def get_lead(**kwargs):
	"""
	Endpoint: GET /api/method/gretok.api.v1.leads.leads.get_lead?name=LEAD-0001
	"""
	kwargs.pop("cmd", None)

	name = kwargs.get("name")
	if not name:
		return error_response(_("name is mandatory"), http_status_code=400)

	if not frappe.db.exists("Leads", name):
		return not_found_response(_("Lead '{0}' does not exist").format(name))

	doc = frappe.get_doc("Leads", name)

	return success_response(
		_("Lead fetched successfully"),
		data={"lead": _build_lead_response(doc)},
	)


# ── UPDATE STATUS ─────────────────────────────────────────────────────────────

@frappe.whitelist()
def update_lead_status(**kwargs):
	"""
	Endpoint: PUT /api/method/gretok.api.v1.leads.leads.update_lead_status

	Body:
		name (str): Lead ID
		status (str): Contacted / Partnered / Declined
	"""
	kwargs.pop("cmd", None)

	log_info(LOG_TITLE, "Update Status Request", kwargs)

	name = kwargs.get("name")
	status = kwargs.get("status")

	if not name:
		return error_response(_("name is mandatory"), http_status_code=400)

	if not status:
		return error_response(_("status is mandatory"), http_status_code=400)

	if status not in ALLOWED_VALUES["status"]:
		return error_response(
			_("status must be one of: {0}").format(", ".join(ALLOWED_VALUES["status"])),
			http_status_code=400,
		)

	if not frappe.db.exists("Leads", name):
		return not_found_response(_("Lead '{0}' does not exist").format(name))

	try:
		doc = frappe.get_doc("Leads", name)
		doc.status = status
		doc.save(ignore_permissions=True)
		frappe.db.commit()
	except Exception as e:
		log_error(LOG_TITLE, "Update Status Failed", kwargs, exc=e)
		return error_response(_("Failed to update Lead status: {0}").format(str(e)), http_status_code=500)

	response = success_response(
		_("Lead status updated successfully"),
		data={"lead": _build_lead_response(doc)},
	)

	log_info(LOG_TITLE, "Update Status Response", response)

	return response


# ── APPROVE ───────────────────────────────────────────────────────────────────

@frappe.whitelist()
def approve_lead(**kwargs):
	"""
	Endpoint: POST /api/method/gretok.api.v1.leads.leads.approve_lead

	Body:
		name (str): Lead ID

	Workflow:
		1. Validate lead exists and current status
		2. Set status to Partnered
		3. Create Partners record
		4. Create Solar Farm Project or BESS Project based on project_type
	"""
	kwargs.pop("cmd", None)

	log_info(LOG_TITLE, "Approve Request", kwargs)

	name = kwargs.get("name")
	if not name:
		return error_response(_("name is mandatory"), http_status_code=400)

	if not frappe.db.exists("Leads", name):
		return not_found_response(_("Lead '{0}' does not exist").format(name))

	doc = frappe.get_doc("Leads", name)

	if doc.status == "Partnered":
		return conflict_response(_("Lead '{0}' is already approved").format(name))

	if doc.status == "Declined":
		return error_response(
			_("Lead '{0}' has been declined and cannot be approved").format(name),
			http_status_code=400,
		)

	try:
		# Step 1 — Update lead status
		doc.status = "Partnered"
		doc.save(ignore_permissions=True)

		# Step 2 — Create Partner
		partner = _create_partner(doc)

		# Step 3 — Create Project based on project_type
		project = None
		if doc.project_type == "Solar Farm":
			project = _create_solar_farm_project(doc, partner)
		elif doc.project_type == "BESS":
			project = _create_bess_project(doc, partner)

		frappe.db.commit()

	except Exception as e:
		log_error(LOG_TITLE, "Approve Failed", kwargs, exc=e)
		return error_response(_("Failed to approve Lead: {0}").format(str(e)), http_status_code=500)

	response = success_response(
		_("Lead approved successfully"),
		data={
			"lead": _build_lead_response(doc),
			"partner": partner,
			"project": project,
		},
	)

	log_info(LOG_TITLE, "Approve Response", response)

	return response


# ── REJECT ────────────────────────────────────────────────────────────────────

@frappe.whitelist()
def reject_lead(**kwargs):
	"""
	Endpoint: POST /api/method/gretok.api.v1.leads.leads.reject_lead

	Body:
		name (str): Lead ID
	"""
	kwargs.pop("cmd", None)

	log_info(LOG_TITLE, "Reject Request", kwargs)

	name = kwargs.get("name")
	if not name:
		return error_response(_("name is mandatory"), http_status_code=400)

	if not frappe.db.exists("Leads", name):
		return not_found_response(_("Lead '{0}' does not exist").format(name))

	doc = frappe.get_doc("Leads", name)

	if doc.status == "Declined":
		return conflict_response(_("Lead '{0}' is already declined").format(name))

	if doc.status == "Partnered":
		return error_response(
			_("Lead '{0}' is already approved and cannot be declined").format(name),
			http_status_code=400,
		)

	try:
		doc.status = "Declined"
		doc.save(ignore_permissions=True)
		frappe.db.commit()

	except Exception as e:
		log_error(LOG_TITLE, "Reject Failed", kwargs, exc=e)
		return error_response(_("Failed to reject Lead: {0}").format(str(e)), http_status_code=500)

	response = success_response(
		_("Lead rejected successfully"),
		data={"lead": _build_lead_response(doc)},
	)

	log_info(LOG_TITLE, "Reject Response", response)

	return response


# ── PRIVATE HELPERS ───────────────────────────────────────────────────────────

def _create_partner(lead_doc):
	"""Create a Partners record from an approved Lead."""
	partner_doc = frappe.get_doc({
		"doctype": "Partners",
		"organization_name": lead_doc.organization_name,
		"email_id": lead_doc.email_id,
		"phone_number": lead_doc.phone_number,
		"asset_category": lead_doc.asset_category,
		"company_gst": lead_doc.company_gst,
		"company_pan": lead_doc.company_pan,
	})
	partner_doc.insert(ignore_permissions=True)

	return {
		"name": partner_doc.name,
		"organization_name": partner_doc.organization_name,
		"email_id": partner_doc.email_id,
	}


def _create_solar_farm_project(lead_doc, partner):
	"""Create a Solar Farm Project from lead data."""
	project_doc = frappe.get_doc({
		"doctype": "Solar Farm Project",
		"project_name": lead_doc.project_name or lead_doc.organization_name,
		"partner": partner.get("name"),
		# Mandatory fields — set defaults, partner can update later
		"solar_farm_type": "Ground-Mounted",
		"commission_date": frappe.utils.today(),
		"crediting_period_start_date": frappe.utils.today(),
		"crediting_period_end_date": frappe.utils.add_years(frappe.utils.today(), 7),
		"location_state": "India",
		"location_district": "TBD",
		"gps_latitude": 0.0,
		"gps_longitude": 0.0,
		"dc_installed_capacity_mwp": 0.0,
		"ac_installed_capacity_mw": 0.0,
	})
	project_doc.insert(ignore_permissions=True)

	return {
		"name": project_doc.name,
		"project_type": "Solar Farm",
		"project_name": project_doc.project_name,
	}


def _create_bess_project(lead_doc, partner):
	"""Create a BESS Project from lead data."""
	project_doc = frappe.get_doc({
		"doctype": "BESS Project",
		"partner": partner.get("name"),
		"project_name": lead_doc.project_name or lead_doc.organization_name,
		# Mandatory fields — set defaults, partner can update later
		"bess_configuration": "Standalone Grid-Connected",
		"commission_date": frappe.utils.today(),
		"crediting_period_start_date": frappe.utils.today(),
		"crediting_period_end_date": frappe.utils.add_years(frappe.utils.today(), 7),
		"location_state": "India",
		"location_district": "TBD",
		"gps_latitude": 0.0,
		"gps_longitude": 0.0,
		"battery_technology": "LFP",
		"rated_energy_capacity_kwh": 0.0,
		"rated_power_output_kw": 0.0,
	})
	project_doc.insert(ignore_permissions=True)

	return {
		"name": project_doc.name,
		"project_type": "BESS",
		"project_name": project_doc.project_name,
	}


def _build_lead_response(doc):
	return {
		"name": doc.name,
		"organization_name": doc.organization_name,
		"email_id": doc.email_id,
		"phone_number": doc.phone_number,
		"status": doc.status,
		"project_name": doc.project_name,
		"project_description": doc.project_description,
		"project_type": doc.project_type,
		"project_specfic_data": doc.project_specfic_data,
		"asset_category": doc.asset_category,
		"company_gst": doc.company_gst,
		"company_pan": doc.company_pan,
		"creation": str(doc.creation),
		"modified": str(doc.modified),
	}