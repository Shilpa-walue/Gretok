import json
import frappe
from frappe import _

from gretok.schemas.v1.bess.rt_data import MANDATORY_FIELDS, OPTIONAL_FIELDS, ALLOWED_VALUES, DEFAULTS
from gretok.utils.validator import validate_payload
from gretok.utils.response import success_response, error_response, not_found_response
from gretok.utils.logger import log_info, log_error

LOG_TITLE = "BESS RT Data API"


@frappe.whitelist()
def store_bess_rt_data(**kwargs):
	"""
	Endpoint: POST /api/method/gretok.api.v1.bess.rt_data.store_bess_rt_data
	"""
	kwargs.pop("cmd", None)

	log_info(LOG_TITLE, "Incoming Request", kwargs)

	error = validate_payload(kwargs, MANDATORY_FIELDS, ALLOWED_VALUES)
	if error:
		return error

	project = kwargs.get("project")
	if not frappe.db.exists("BESS Project", project):
		return not_found_response(_("BESS Project '{0}' does not exist").format(project))

	doc_data = {"doctype": "BESS RT Data"}

	for field in MANDATORY_FIELDS:
		doc_data[field] = kwargs.get(field)

	for field in OPTIONAL_FIELDS:
		value = kwargs.get(field)
		if value is not None:
			doc_data[field] = value
		elif field in DEFAULTS:
			doc_data[field] = DEFAULTS[field]

	try:
		doc = frappe.get_doc(doc_data)
		doc.insert(ignore_permissions=True)
		frappe.db.commit()
	except Exception as e:
		log_error(LOG_TITLE, "Insert Failed", kwargs, exc=e)
		return error_response(_("Failed to store BESS RT Data: {0}").format(str(e)), http_status_code=500)

	response_data = _build_rt_response(doc)

	response = success_response(
		_("BESS RT Data stored successfully"),
		data={"rt_data": response_data},
	)

	log_info(LOG_TITLE, "Response", response)

	return response


@frappe.whitelist()
def store_bess_rt_data_batch(**kwargs):
	"""
	Endpoint: POST /api/method/gretok.api.v1.bess.rt_data.store_bess_rt_data_batch
	"""
	kwargs.pop("cmd", None)

	records = kwargs.get("records") or []
	if isinstance(records, str):
		records = json.loads(records)

	if not records:
		return error_response(_("No records provided"), http_status_code=400)

	log_info(LOG_TITLE, "Batch Incoming", {"count": len(records)})

	project = records[0].get("project") if records else None
	if project and not frappe.db.exists("BESS Project", project):
		return not_found_response(_("BESS Project '{0}' does not exist").format(project))

	created = []
	errors = []

	for idx, record in enumerate(records):
		try:
			error = validate_payload(record, MANDATORY_FIELDS, ALLOWED_VALUES)
			if error:
				errors.append({"index": idx, "timestamp": record.get("timestamp"), "error": error})
				continue

			doc_data = {"doctype": "BESS RT Data"}

			for field in MANDATORY_FIELDS:
				doc_data[field] = record.get(field)

			for field in OPTIONAL_FIELDS:
				value = record.get(field)
				if value is not None:
					doc_data[field] = value
				elif field in DEFAULTS:
					doc_data[field] = DEFAULTS[field]

			doc = frappe.get_doc(doc_data)
			doc.insert(ignore_permissions=True)
			created.append(doc.name)

		except Exception as e:
			log_error(LOG_TITLE, f"Batch Insert Failed at index {idx}", record, exc=e)
			errors.append({"index": idx, "timestamp": record.get("timestamp"), "error": str(e)})

	frappe.db.commit()

	response = success_response(
		_("{0} records created, {1} failed").format(len(created), len(errors)),
		data={
			"created_count": len(created),
			"error_count": len(errors),
			"created": created,
			"errors": errors,
		},
	)

	log_info(LOG_TITLE, "Batch Response", response)

	return response


def _build_rt_response(doc):
	return {
		"name": doc.name,
		"project": doc.project,
		"timestamp": str(doc.timestamp) if doc.timestamp else None,
		"system_operating_mode": doc.system_operating_mode,
		"pcs_inverter_status": doc.pcs_inverter_status,
		"system_availability_flag": doc.system_availability_flag,
		"battery_soc_pct": doc.battery_soc_pct,
		"battery_soh_pct": doc.battery_soh_pct,
		"battery_cycle_count": doc.battery_cycle_count,
		"battery_rated_capacity_kwh": doc.battery_rated_capacity_kwh,
		"battery_charge_power_kw": doc.battery_charge_power_kw,
		"battery_discharge_power_kw": doc.battery_discharge_power_kw,
		"round_trip_efficiency_interval_pct": doc.round_trip_efficiency_interval_pct,
		"grid_export_energy_kwh": doc.grid_export_energy_kwh,
		"grid_import_energy_kwh": doc.grid_import_energy_kwh,
		"active_power_export_kw": doc.active_power_export_kw,
		"solar_generation_input_kw": doc.solar_generation_input_kw,
		"auxiliary_consumption_kw": doc.auxiliary_consumption_kw,
		"battery_cell_temp_avg_c": doc.battery_cell_temp_avg_c,
		"battery_cell_temp_max_c": doc.battery_cell_temp_max_c,
		"ambient_temperature_c": doc.ambient_temperature_c,
		"solar_irradiance_ghi_wm2": doc.solar_irradiance_ghi_wm2,
		"bms_alarm_flag": doc.bms_alarm_flag,
	}
