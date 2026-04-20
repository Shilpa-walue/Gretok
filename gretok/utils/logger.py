import json
import frappe


def log_info(title, event, data=None):
	"""
	Log an informational message to the Frappe error log.

	Args:
		title (str): Module or API name. e.g. "Solar Farm Project API"
		event (str): What is happening. e.g. "Incoming Request", "Response"
		data (any): Payload to log. Dict, list, or string.

	Usage:
		log_info("Solar Farm Project API", "Incoming Request", kwargs)
		log_info("Solar Farm Project API", "Response", response)
	"""
	try:
		if isinstance(data, (dict, list)):
			message = json.dumps(data, indent=2, default=str)
		else:
			message = str(data) if data is not None else ""

		frappe.logger("gretok").info(
			f"\n{'=' * 60}\n"
			f"[{title}] {event}\n"
			f"{'-' * 60}\n"
			f"{message}\n"
			f"{'=' * 60}"
		)
	except Exception:
		pass


def log_error(title, event, data=None, exc=None):
	"""
	Log an error with optional exception traceback.

	Args:
		title (str): Module or API name.
		event (str): What failed. e.g. "Insert Failed"
		data (any): Payload context at time of error.
		exc (Exception): Optional exception object.

	Usage:
		except Exception as e:
			log_error("Solar Farm Project API", "Insert Failed", kwargs, exc=e)
	"""
	try:
		if isinstance(data, (dict, list)):
			message = json.dumps(data, indent=2, default=str)
		else:
			message = str(data) if data is not None else ""

		error_message = (
			f"\n{'=' * 60}\n"
			f"[{title}] ERROR — {event}\n"
			f"{'-' * 60}\n"
			f"{message}\n"
		)

		if exc:
			error_message += f"\nException: {str(exc)}\n"

		error_message += f"{'=' * 60}"

		frappe.logger("gretok").error(error_message)

		# Also write to Frappe Error Log (visible in desk)
		frappe.log_error(
			title=f"[{title}] {event}",
			message=f"{message}\n\n{str(exc) if exc else ''}",
		)
	except Exception:
		pass


def log_warning(title, event, data=None):
	"""
	Log a warning message.

	Usage:
		log_warning("Solar Farm Monthly Data API", "Budget calculation skipped", {"project": "SF-001"})
	"""
	try:
		if isinstance(data, (dict, list)):
			message = json.dumps(data, indent=2, default=str)
		else:
			message = str(data) if data is not None else ""

		frappe.logger("gretok").warning(
			f"\n{'=' * 60}\n"
			f"[{title}] WARNING — {event}\n"
			f"{'-' * 60}\n"
			f"{message}\n"
			f"{'=' * 60}"
		)
	except Exception:
		pass
