import frappe
from frappe import _

from gretok.schemas.v1.bess.project import MANDATORY_FIELDS, OPTIONAL_FIELDS, ALLOWED_VALUES
from gretok.utils.validator import validate_payload
from gretok.utils.response import success_response, error_response, conflict_response
from gretok.utils.logger import log_info, log_error

LOG_TITLE = "BESS Project API"


@frappe.whitelist()
def store_bess_project(**kwargs):
	"""
	Endpoint: POST /api/method/gretok.api.v1.bess.project.store_bess_project
	"""
	kwargs.pop("cmd", None)

	log_info(LOG_TITLE, "Incoming Request", kwargs)

	error = validate_payload(kwargs, MANDATORY_FIELDS, ALLOWED_VALUES)
	if error:
		return error

	if kwargs.get("bess_configuration") == "Coupled with Renewable":
		if not kwargs.get("coupled_project_reference"):
			return error_response(
				_("coupled_project_reference is mandatory for Coupled with Renewable configuration"),
				http_status_code=400,
			)
		if not frappe.db.exists("Solar Farm Project", kwargs.get("coupled_project_reference")):
			return error_response(
				_("Solar Farm Project '{0}' does not exist").format(kwargs.get("coupled_project_reference")),
				http_status_code=404,
			)

	if frappe.db.exists("BESS Project", {"project_name": kwargs.get("project_name")}):
		return conflict_response(
			_("A BESS Project with name '{0}' already exists").format(kwargs.get("project_name"))
		)

	doc_data = {"doctype": "BESS Project"}

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
		return error_response(_("Failed to create BESS Project: {0}").format(str(e)), http_status_code=500)

	response_data = _build_project_response(doc)

	frappe.publish_realtime("bess_project_created", response_data, after_commit=True)

	response = success_response(
		_("BESS Project created successfully"),
		data={"project": response_data},
	)

	log_info(LOG_TITLE, "Response", response)

	return response


def _build_project_response(doc):
	return {
		"name": doc.name,
		"project_name": doc.project_name,
		"bess_configuration": doc.bess_configuration,
		"coupled_project_reference": doc.coupled_project_reference,
		"commission_date": str(doc.commission_date) if doc.commission_date else None,
		"crediting_period_start_date": str(doc.crediting_period_start_date) if doc.crediting_period_start_date else None,
		"crediting_period_end_date": str(doc.crediting_period_end_date) if doc.crediting_period_end_date else None,
		"location_state": doc.location_state,
		"location_district": doc.location_district,
		"gps_latitude": doc.gps_latitude,
		"gps_longitude": doc.gps_longitude,
		"battery_technology": doc.battery_technology,
		"rated_energy_capacity_kwh": doc.rated_energy_capacity_kwh,
		"rated_power_output_kw": doc.rated_power_output_kw,
		"bess_operating_mode": doc.bess_operating_mode,
		"design_round_trip_efficiency_pct": doc.design_round_trip_efficiency_pct,
	}
