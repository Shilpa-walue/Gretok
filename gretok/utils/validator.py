from frappe import _
from gretok.utils.response import validation_error_response, error_response


def validate_payload(data, mandatory_fields, allowed_values=None):
	"""
	Shared validator used across all v1 APIs.

	Args:
		data (dict): Incoming request payload.
		mandatory_fields (dict): { field_name: expected_type }
		allowed_values (dict): { field_name: [allowed, values, list] }

	Returns:
		None if valid, error_response dict if invalid.
	"""
	# Check mandatory fields
	for field, expected_type in mandatory_fields.items():
		value = data.get(field)

		if value is None or value == "":
			return validation_error_response(
				field,
				_("{0} is mandatory").format(field),
			)

		# Try type coercion — don't fail if string "1.5" is passed for float
		if not isinstance(value, expected_type):
			try:
				expected_type(value)
			except (ValueError, TypeError):
				return validation_error_response(
					field,
					_("{0} must be of type {1}").format(field, expected_type.__name__),
				)

	# Check allowed values for select fields
	if allowed_values:
		for field, allowed in allowed_values.items():
			value = data.get(field)
			if value and value not in allowed:
				return validation_error_response(
					field,
					_("{0} must be one of: {1}").format(field, ", ".join(str(a) for a in allowed)),
				)

	return None
