MANDATORY_FIELDS = {
	"organization_name": str,
	"email_id": str,
	"phone_number": str,
}

OPTIONAL_FIELDS = [
	"status",
	"project_name",
	"project_description",
	"project_type",
	"project_specfic_data",
	"asset_category",
	"company_gst",
	"company_pan",
]

ALLOWED_VALUES = {
	"status": ["Contacted", "Partnered", "Declined"],
	"project_type": ["Solar Farm", "BESS"],
}

DEFAULTS = {
	"status": "Contacted",
}
