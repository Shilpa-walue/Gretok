import json
import frappe
from frappe import _

from gretok.schemas.v1.solar_farm.rt_data import MANDATORY_FIELDS, OPTIONAL_FIELDS, ALLOWED_VALUES, DEFAULTS
from gretok.utils.validator import validate_payload
from gretok.utils.response import success_response, error_response
from gretok.utils.logger import log_info

LOG_TITLE = "Solar Farm RT Data API"


@frappe.whitelist()
def store_solar_farm_rt_data(**kwargs):
	"""
	API to store a single Solar Farm Real-Time SCADA record (every 15 mins).

	Endpoint: POST /api/method/gretok.api.v1.solar_farm.rt_data.store_solar_farm_rt_data
	"""
	kwargs.pop("cmd", None)

	log_info(LOG_TITLE, "Incoming Request", kwargs)

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

	doc_data = {"doctype": "Solar Farm RT Data"}

	for field in MANDATORY_FIELDS:
		doc_data[field] = kwargs.get(field)

	for field in OPTIONAL_FIELDS:
		value = kwargs.get(field)
		if value is not None:
			doc_data[field] = value
		elif field in DEFAULTS:
			doc_data[field] = DEFAULTS[field]

	doc = frappe.get_doc(doc_data)
	doc.insert(ignore_permissions=True)
	frappe.db.commit()

	response_data = _build_rt_response(doc)

	response = success_response(
		_("Solar Farm RT Data stored successfully"),
		data={"rt_data": response_data},
	)

	log_info(LOG_TITLE, "Response", response)

	return response


@frappe.whitelist()
def store_solar_farm_rt_data_batch(**kwargs):
	"""
	Batch API to store multiple RT SCADA records in one call.

	Endpoint: POST /api/method/gretok.api.v1.solar_farm.rt_data.store_solar_farm_rt_data_batch

	Args:
		records (list/str): List of RT data objects.
	"""
	kwargs.pop("cmd", None)

	records = kwargs.get("records") or []
	if isinstance(records, str):
		records = json.loads(records)

	if not records:
		return error_response(_("No records provided"), http_status_code=400)

	log_info(LOG_TITLE, "Batch Incoming", {"count": len(records)})

	# Validate project exists once (assume all records belong to same project)
	project = records[0].get("project") if records else None
	if project and not frappe.db.exists("Solar Farm Project", project):
		return error_response(
			_("Solar Farm Project '{0}' does not exist").format(project),
			http_status_code=404,
		)

	created = []
	errors = []

	for idx, record in enumerate(records):
		try:
			error = validate_payload(record, MANDATORY_FIELDS, ALLOWED_VALUES)
			if error:
				errors.append({"index": idx, "timestamp": record.get("timestamp"), "error": error})
				continue

			doc_data = {"doctype": "Solar Farm RT Data"}

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
		"inverter_status": doc.inverter_status,
		"grid_availability_flag": doc.grid_availability_flag,
		"active_power_generation_kw": doc.active_power_generation_kw,
		"energy_generated_interval_kwh": doc.energy_generated_interval_kwh,
		"active_power_export_kw": doc.active_power_export_kw,
		"grid_frequency_hz": doc.grid_frequency_hz,
		"solar_irradiance_ghi_wm2": doc.solar_irradiance_ghi_wm2,
		"module_temperature_c": doc.module_temperature_c,
		"ambient_temperature_c": doc.ambient_temperature_c,
	}
