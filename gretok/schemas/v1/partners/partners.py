MANDATORY_FIELDS = {
	"organization_name": str,
	"email_id": str,
}

OPTIONAL_FIELDS = [
	"phone_number",
	"company_legal_name",
	"company_registration_number",
	"company_type",
	"country_of_registration",
	"state_of_registration",
	"registered_office_address",
	"company_gst",
	"company_pan",
	"asset_category",
	"primary_contact_name",
	"primary_contact_email",
	"primary_contact_phone",
	"total_partner_tokens",
	"carbon_credits",
	"wallet_address",
	"gtka",
	"gtk",
	"gtkcc",
	"gtkgp",
	"total_carbon_saved",
]

ALLOWED_VALUES = {
	"company_type": [
		"Independent Power Producer (IPP)",
		"Corporate (Captive)",
		"Project Developer",
		"EPC Contractor",
	],
}