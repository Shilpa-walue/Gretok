import frappe
from frappe import _

from gretok.schemas.v1.solar_farm.monthly_data import MANDATORY_FIELDS, OPTIONAL_FIELDS, ALLOWED_VALUES
from gretok.utils.validator import validate_payload
from gretok.utils.response import success_response, error_response
from gretok.utils.logger import log_info

LOG_TITLE = "Solar Farm Monthly Data API"


@frappe.whitelist()
def store_solar_farm_monthly_data(**kwargs):
	"""
	API to store Solar Farm Monthly Data (submitted by Partner).

	Endpoint: POST /api/method/gretok.api.v1.solar_farm.monthly_data.store_solar_farm_monthly_data
	"""
	kwargs.pop("cmd", None)

	log_info(LOG_TITLE, "Incoming Request", kwargs)

	# Validate
	error = validate_payload(kwargs, MANDATORY_FIELDS, ALLOWED_VALUES)
	if error:
		return error

	# Check project exists
	project = kwargs.get("project")
	if not frappe.db.exists("Solar Farm Project", project):
		return error_response(
			_("Solar Farm Project '{0}' does not exist").format(project),
			http_status_code=404,
		)

	# Check duplicate: same project + same reporting month
	if frappe.db.exists("Solar Farm Monthly Data", {
		"project": project,
		"reporting_month": kwargs.get("reporting_month"),
	}):
		return error_response(
			_("Monthly data for project '{0}' and month '{1}' already exists").format(
				project, kwargs.get("reporting_month")
			),
			http_status_code=409,
		)

	# Build doc
	doc_data = {"doctype": "Solar Farm Monthly Data"}

	for field in MANDATORY_FIELDS:
		doc_data[field] = kwargs.get(field)

	for field in OPTIONAL_FIELDS:
		value = kwargs.get(field)
		if value is not None:
			doc_data[field] = value

	# Calculate derived fields
	opening = float(kwargs.get("opening_meter_reading_kwh") or 0)
	closing = float(kwargs.get("closing_meter_reading_kwh") or 0)
	auxiliary = float(kwargs.get("auxiliary_consumption_kwh") or 0)

	gross_energy = closing - opening
	net_energy = gross_energy - auxiliary

	doc_data["gross_energy_generated_kwh"] = round(gross_energy, 2)
	doc_data["net_energy_exported_kwh"] = round(net_energy, 2)

	# Calculate system availability if downtime provided
	planned = float(kwargs.get("planned_downtime_hours") or 0)
	unplanned = float(kwargs.get("unplanned_downtime_hours") or 0)
	operational_days = int(kwargs.get("plant_operational_days") or 0)
	if operational_days:
		total_hours = operational_days * 24
		available_hours = total_hours - planned - unplanned
		doc_data["system_availability"] = round((available_hours / total_hours) * 100, 2) if total_hours else 0

	doc = frappe.get_doc(doc_data)
	doc.insert(ignore_permissions=True)
	frappe.db.commit()

	response_data = _build_monthly_response(doc)

	frappe.publish_realtime(
		"solar_farm_monthly_data_stored",
		response_data,
		after_commit=True,
	)

	response = success_response(
		_("Solar Farm Monthly Data stored successfully"),
		data={"monthly_data": response_data},
	)

	log_info(LOG_TITLE, "Response", response)

	return response


def _build_monthly_response(doc):
	return {
		"name": doc.name,
		"project": doc.project,
		"reporting_month": str(doc.reporting_month) if doc.reporting_month else None,
		"opening_meter_reading_kwh": doc.opening_meter_reading_kwh,
		"closing_meter_reading_kwh": doc.closing_meter_reading_kwh,
		"gross_energy_generated_kwh": doc.gross_energy_generated_kwh,
		"auxiliary_consumption_kwh": doc.auxiliary_consumption_kwh,
		"net_energy_exported_kwh": doc.net_energy_exported_kwh,
		"grid_curtailment_energy_lost_kwh": doc.grid_curtailment_energy_lost_kwh,
		"plant_operational_days": doc.plant_operational_days,
		"planned_downtime_hours": doc.planned_downtime_hours,
		"unplanned_downtime_hours": doc.unplanned_downtime_hours,
		"system_availability": doc.system_availability,
	}
