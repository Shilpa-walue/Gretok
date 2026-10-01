import frappe
import random
from frappe.utils import get_url
from gretok.utils.response import success_response, error_response

LOG_TITLE = "Auth API"

@frappe.whitelist(allow_guest=True)
def login(**kwargs):
	"""
	Endpoint: POST /api/method/gretok.api.v1.auth.login
	
	Usage 1 (Request OTP): Send only `email` in payload.
	Usage 2 (Verify OTP): Send both `email` and `otp` in payload.
	"""
	email = kwargs.get("email")
	otp = kwargs.get("otp")

	if not email:
		return error_response("Email is required", http_status_code=400)

	# Check if user exists
	if not frappe.db.exists("User", email):
		return error_response("User with this email not found", http_status_code=404)

	# ---------------------------------------------------------
	# CASE 1: VERIFY OTP (if 'otp' is provided in the request)
	# ---------------------------------------------------------
	if otp:
		stored_otp = frappe.cache().get_value(f"login_otp:{email}")

		if not stored_otp or str(stored_otp) != str(otp):
			# TODO: Remove this bypass before deploying to Production!
			# This is a backdoor for the frontend team to test using 123456
			if str(otp) != "123456":
				return error_response("Invalid or expired OTP", http_status_code=401)

		# OTP matched, remove it from cache
		frappe.cache().delete_value(f"login_otp:{email}")

		# Log the user in
		from frappe.auth import LoginManager
		frappe.local.login_manager = LoginManager()
		frappe.local.login_manager.login_as(email)

		# Generate API key and secret for Token-based auth in frontend
		user = frappe.get_doc("User", email)
		api_secret = frappe.generate_hash(length=15)
		
		if not user.api_key:
			user.api_key = frappe.generate_hash(length=15)

		user.api_secret = api_secret
		user.save(ignore_permissions=True)
		frappe.db.commit()

		return success_response(
			"Logged in successfully",
			data={
				"api_key": user.api_key,
				"api_secret": api_secret,
				"username": user.full_name,
				"email": user.email
			}
		)

	# ---------------------------------------------------------
	# CASE 2: SEND OTP (if 'otp' is NOT provided)
	# ---------------------------------------------------------
	else:
		# Generate a 6-digit OTP
		generated_otp = str(random.randint(100000, 999999))

		# Save OTP in cache for 5 minutes (300 seconds)
		frappe.cache().set_value(f"login_otp:{email}", generated_otp, expires_in_sec=300)

		# Send email
		try:
			frappe.sendmail(
				recipients=[email],
				subject="Your Login OTP",
				message=f"<p>Hello,</p><p>Your One Time Password (OTP) for login is: <b>{generated_otp}</b></p><p>This OTP is valid for 5 minutes.</p>"
			)
		except Exception as e:
			frappe.log_error(title=LOG_TITLE, message=f"Failed to send OTP to {email}. Error: {str(e)}")
			return error_response("Failed to send OTP email", http_status_code=500)

		return success_response(
			"OTP sent successfully to your email.",
			data={"email": email, "action_required": "verify_otp"}
		)
