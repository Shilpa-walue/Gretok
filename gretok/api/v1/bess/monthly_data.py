import frappe
from frappe import _

from gretok.schemas.v1.bess.monthly_data import MANDATORY_FIELDS, OPTIONAL_FIELDS, ALLOWED_VALUES
from gretok.utils.validator import validate_payload
from gretok.utils.response import success_response, error_response, conflict_response, not_found_response
from gretok.utils.logger import log_info, log_error

LOG_TITLE = "BESS Monthly Data API"


@frappe.whitelist()
def store_bess_monthly_data(**kwargs):
	"""
	Endpoint: POST /api/method/gretok.api.v1.bess.monthly_data.store_bess_monthly_data
	"""
	kwargs.pop("cmd", None)

	log_info(LOG_TITLE, "Incoming Request", kwargs)

	error = validate_payload(kwargs, MANDATORY_FIELDS, ALLOWED_VALUES)
	if error:
		return error

	project = kwargs.get("project")
	if not frappe.db.exists("BESS Project", project):
		return not_found_response(_("BESS Project '{0}' does not exist").format(project))

	if frappe.db.exists("BESS Monthly Data", {
		"project": project,
		"reporting_month": kwargs.get("reporting_month"),
	}):
		return conflict_response(
			_("Monthly data for project '{0}' and month '{1}' already exists").format(
				project, kwargs.get("reporting_month")
			)
		)

	doc_data = {"doctype": "BESS Monthly Data"}

	for field in MANDATORY_FIELDS:
		doc_data[field] = kwargs.get(field)

	for field in OPTIONAL_FIELDS:
		value = kwargs.get(field)
		if value is not None:
			doc_data[field] = value

	energy_charged_solar = float(kwargs.get("total_energy_charged_from_solar_kwh") or 0)
	energy_charged_grid = float(kwargs.get("total_energy_charged_from_grid_kwh") or 0)
	energy_discharged = float(kwargs.get("total_energy_discharged_to_grid_kwh") or 0)
	total_charged = energy_charged_solar + energy_charged_grid

	if total_charged > 0:
		doc_data["actual_round_trip_efficiency_pct"] = round((energy_discharged / total_charged) * 100, 2)

	project_doc = frappe.get_doc("BESS Project", project)
	rated_capacity = project_doc.rated_energy_capacity_kwh or 0
	if rated_capacity > 0:
		doc_data["number_of_full_equivalent_cycles"] = round(energy_discharged / rated_capacity, 2)

	planned = float(kwargs.get("planned_downtime_hours") or 0)
	unplanned = float(kwargs.get("unplanned_downtime_hours") or 0)
	total_hours = 30 * 24
	available_hours = total_hours - planned - unplanned
	doc_data["system_availability_pct"] = round((available_hours / total_hours) * 100, 2)

	try:
		doc = frappe.get_doc(doc_data)
		doc.insert(ignore_permissions=True)
		frappe.db.commit()
	except Exception as e:
		log_error(LOG_TITLE, "Insert Failed", kwargs, exc=e)
		return error_response(_("Failed to store BESS Monthly Data: {0}").format(str(e)), http_status_code=500)

	response_data = _build_monthly_response(doc)

	frappe.publish_realtime("bess_monthly_data_stored", response_data, after_commit=True)

	response = success_response(
		_("BESS Monthly Data stored successfully"),
		data={"monthly_data": response_data},
	)

	log_info(LOG_TITLE, "Response", response)

	return response


def _build_monthly_response(doc):
	return {
		"name": doc.name,
		"project": doc.project,
		"reporting_month": str(doc.reporting_month) if doc.reporting_month else None,
		"total_energy_charged_from_solar_kwh": doc.total_energy_charged_from_solar_kwh,
		"total_energy_charged_from_grid_kwh": doc.total_energy_charged_from_grid_kwh,
		"total_energy_discharged_to_grid_kwh": doc.total_energy_discharged_to_grid_kwh,
		"auxiliary_consumption_kwh": doc.auxiliary_consumption_kwh,
		"actual_round_trip_efficiency_pct": doc.actual_round_trip_efficiency_pct,
		"number_of_full_equivalent_cycles": doc.number_of_full_equivalent_cycles,
		"average_state_of_health_pct": doc.average_state_of_health_pct,
		"system_availability_pct": doc.system_availability_pct,
		"planned_downtime_hours": doc.planned_downtime_hours,
		"unplanned_downtime_hours": doc.unplanned_downtime_hours,
		"bms_alarm_events_count": doc.bms_alarm_events_count,
		"curtailment_duration_hours": doc.curtailment_duration_hours,
		"curtailment_energy_lost_kwh": doc.curtailment_energy_lost_kwh,
	}
