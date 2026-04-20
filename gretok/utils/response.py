import frappe


def success_response(message, data=None, http_status_code=200):
	"""
	Standard success response.

	Shape:
	{
		"status": "success",
		"message": "...",
		"data": { ... },
		"http_status_code": 200
	}
	"""
	frappe.local.response["http_status_code"] = http_status_code

	response = {
		"status": "success",
		"message": message,
		"data": data or {},
	}

	return response


def error_response(message, errors=None, http_status_code=400):
	"""
	Standard error response.

	Shape:
	{
		"status": "error",
		"message": "...",
		"errors": [ ... ],
		"http_status_code": 400
	}
	"""
	frappe.local.response["http_status_code"] = http_status_code

	response = {
		"status": "error",
		"message": message,
		"errors": errors or [],
	}

	return response


def not_found_response(message):
	"""404 shorthand."""
	return error_response(message, http_status_code=404)


def conflict_response(message):
	"""409 shorthand."""
	return error_response(message, http_status_code=409)


def unauthorized_response(message="Unauthorized"):
	"""401 shorthand."""
	return error_response(message, http_status_code=401)


def validation_error_response(field, message):
	"""
	400 with field-level error detail.

	Shape:
	{
		"status": "error",
		"message": "Validation failed",
		"errors": [
			{ "field": "project_name", "message": "project_name is mandatory" }
		]
	}
	"""
	return error_response(
		"Validation failed",
		errors=[{"field": field, "message": message}],
		http_status_code=400,
	)
