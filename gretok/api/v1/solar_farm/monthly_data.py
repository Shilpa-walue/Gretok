import frappe
from frappe import _

from gretok.schemas.v1.solar_farm.monthly_data import MANDATORY_FIELDS, OPTIONAL_FIELDS, ALLOWED_VALUES
from gretok.utils.validator import validate_payload
from gretok.utils.response import success_response, error_response, not_found_response, conflict_response
from gretok.utils.logger import log_info, log_error

LOG_TITLE = "Solar Farm Monthly Data API"


# ── CREATE ────────────────────────────────────────────────────────────────────

@frappe.whitelist()
def store_solar_farm_monthly_data(**kwargs):
	kwargs.pop("cmd", None)

	log_info(LOG_TITLE, "Incoming Request", kwargs)

	error = validate_payload(kwargs, MANDATORY_FIELDS, ALLOWED_VALUES)
	if error:
		return error

	project = kwargs.get("project")
	if not frappe.db.exists("Solar Farm Project", project):
		return not_found_response(_("Solar Farm Project '{0}' does not exist").format(project))

	if frappe.db.exists("Solar Farm Monthly Data", {
		"project": project,
		"reporting_month": kwargs.get("reporting_month"),
	}):
		return conflict_response(
			_("Monthly data for project '{0}' and month '{1}' already exists").format(
				project, kwargs.get("reporting_month")
			)
		)

	doc_data = {"doctype": "Solar Farm Monthly Data"}

	for field in MANDATORY_FIELDS:
		doc_data[field] = kwargs.get(field)

	for field in OPTIONAL_FIELDS:
		value = kwargs.get(field)
		if value is not None:
			doc_data[field] = value

	opening = float(kwargs.get("opening_meter_reading_kwh") or 0)
	closing = float(kwargs.get("closing_meter_reading_kwh") or 0)
	auxiliary = float(kwargs.get("auxiliary_consumption_kwh") or 0)
	gross_energy = closing - opening
	net_energy = gross_energy - auxiliary
	doc_data["gross_energy_generated_kwh"] = round(gross_energy, 2)
	doc_data["net_energy_exported_kwh"] = round(net_energy, 2)

	planned = float(kwargs.get("planned_downtime_hours") or 0)
	unplanned = float(kwargs.get("unplanned_downtime_hours") or 0)
	operational_days = int(kwargs.get("plant_operational_days") or 0)
	if operational_days:
		total_hours = operational_days * 24
		available_hours = total_hours - planned - unplanned
		doc_data["system_availability"] = round((available_hours / total_hours) * 100, 2) if total_hours else 0

	try:
		doc = frappe.get_doc(doc_data)
		doc.insert(ignore_permissions=True)
		frappe.db.commit()
	except Exception as e:
		log_error(LOG_TITLE, "Insert Failed", kwargs, exc=e)
		return error_response(_("Failed to store Solar Farm Monthly Data: {0}").format(str(e)), http_status_code=500)

	response_data = _build_monthly_response(doc)

	frappe.publish_realtime("solar_farm_monthly_data_stored", response_data, after_commit=True)

	response = success_response(
		_("Solar Farm Monthly Data stored successfully"),
		data={"monthly_data": response_data},
	)

	log_info(LOG_TITLE, "Response", response)

	return response


# ── FETCH ALL ─────────────────────────────────────────────────────────────────

@frappe.whitelist()
def get_solar_farm_monthly_data_list(**kwargs):
	kwargs.pop("cmd", None)

	limit = min(int(kwargs.get("limit") or 20), 100)
	offset = int(kwargs.get("offset") or 0)

	filters = {}
	if kwargs.get("project"):
		filters["project"] = kwargs.get("project")

	records = frappe.get_all(
		"Solar Farm Monthly Data",
		filters=filters,
		fields=[
			"name", "project", "reporting_month",
			"gross_energy_generated_kwh", "net_energy_exported_kwh",
			"auxiliary_consumption_kwh", "system_availability",
			"plant_operational_days", "creation", "modified",
		],
		limit=limit,
		start=offset,
		order_by="reporting_month desc",
	)

	total = frappe.db.count("Solar Farm Monthly Data", filters=filters)

	return success_response(
		_("Solar Farm Monthly Data fetched successfully"),
		data={
			"records": records,
			"total": total,
			"limit": limit,
			"offset": offset,
		},
	)


# ── FETCH SINGLE ──────────────────────────────────────────────────────────────

@frappe.whitelist()
def get_solar_farm_monthly_data(**kwargs):
	kwargs.pop("cmd", None)

	name = kwargs.get("name")
	if not name:
		return error_response(_("name is mandatory"), http_status_code=400)

	if not frappe.db.exists("Solar Farm Monthly Data", name):
		return not_found_response(_("Solar Farm Monthly Data '{0}' does not exist").format(name))

	doc = frappe.get_doc("Solar Farm Monthly Data", name)

	return success_response(
		_("Solar Farm Monthly Data fetched successfully"),
		data={"monthly_data": _build_monthly_response(doc)},
	)


# ── UPDATE ────────────────────────────────────────────────────────────────────

@frappe.whitelist()
def update_solar_farm_monthly_data(**kwargs):
	kwargs.pop("cmd", None)

	log_info(LOG_TITLE, "Update Request", kwargs)

	name = kwargs.get("name")
	if not name:
		return error_response(_("name is mandatory"), http_status_code=400)

	if not frappe.db.exists("Solar Farm Monthly Data", name):
		return not_found_response(_("Solar Farm Monthly Data '{0}' does not exist").format(name))

	try:
		doc = frappe.get_doc("Solar Farm Monthly Data", name)

		for field in OPTIONAL_FIELDS:
			value = kwargs.get(field)
			if value is not None:
				setattr(doc, field, value)

		opening = float(doc.opening_meter_reading_kwh or 0)
		closing = float(doc.closing_meter_reading_kwh or 0)
		auxiliary = float(doc.auxiliary_consumption_kwh or 0)
		gross_energy = closing - opening
		doc.gross_energy_generated_kwh = round(gross_energy, 2)
		doc.net_energy_exported_kwh = round(gross_energy - auxiliary, 2)

		doc.save(ignore_permissions=True)
		frappe.db.commit()
	except Exception as e:
		log_error(LOG_TITLE, "Update Failed", kwargs, exc=e)
		return error_response(_("Failed to update Solar Farm Monthly Data: {0}").format(str(e)), http_status_code=500)

	response = success_response(
		_("Solar Farm Monthly Data updated successfully"),
		data={"monthly_data": _build_monthly_response(doc)},
	)

	log_info(LOG_TITLE, "Update Response", response)

	return response


# ── HELPERS ───────────────────────────────────────────────────────────────────

def _build_monthly_response(doc):
	return {
		"name": doc.name,
		"project": doc.project,
		"reporting_month": str(doc.reporting_month) if doc.reporting_month else None,
		"opening_meter_reading_kwh": doc.opening_meter_reading_kwh,
		"opening_reading_datetime": str(doc.opening_reading_datetime) if doc.opening_reading_datetime else None,
		"closing_meter_reading_kwh": doc.closing_meter_reading_kwh,
		"closing_reading_datetime": str(doc.closing_reading_datetime) if doc.closing_reading_datetime else None,
		"utility_representative_name": doc.utility_representative_name,
		"company_representative_name": doc.company_representative_name,
		"gross_energy_generated_kwh": doc.gross_energy_generated_kwh,
		"net_energy_exported_kwh": doc.net_energy_exported_kwh,
		"auxiliary_consumption_kwh": doc.auxiliary_consumption_kwh,
		"grid_curtailment_energy_lost_kwh": doc.grid_curtailment_energy_lost_kwh,
		"plant_operational_days": doc.plant_operational_days,
		"planned_downtime_hours": doc.planned_downtime_hours,
		"unplanned_downtime_hours": doc.unplanned_downtime_hours,
		"actual_performance_ratio": doc.actual_performance_ratio,
		"actual_capacity_utilization_factor": doc.actual_capacity_utilization_factor,
		"system_availability": doc.system_availability,
		"creation": str(doc.creation),
		"modified": str(doc.modified),
	}