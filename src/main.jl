using JSON
using HTTP

######################################## Application Parameters ########################################

#################### Working Directory ####################
if gethostname() == "BORData"
    cd("C:\\Users\\jmbyars\\Documents\\BrightSpaceAPI")
else
    cd("/mnt/8401f199-95d5-43e7-b83f-ce2ec42b020b/jmb/GIT/BrightSpaceAPI/")
end

#################### Configuration ####################
credentials = JSON.parsefile("credentials.json")

#################### Specify Datasets ####################

########## Full Datasets ##########
FULL_DATASETS_META = Dict(
    "org_units" => "07a9e561-e22f-4e82-8dd6-7bfb14c91776'",
    "grade_objects" => "793668a8-2c58-4e5e-b263-412d28d5703f",
    "users" => "1d6d722e-b572-456f-97c1-d526570daa6b",
    "grade_results" => "9d8a96b4-8145-416d-bd18-11402bc58f8d",
    "user_enrollments" => "533f84c8-b2ad-4688-94dc-c839952e9c4f"
)

########## Differential Datasets ##########
DIFF_DATASETS_META = Dict(
    "user_enrollments" => "a78735f2-7210-4a57-aac1-e0f6bd714349"
)

######################################## API Functions ########################################

#################### Trade In Refresh Token ####################
#function trade_in_refresh_token(credentials::Dict{String, Any})

config = credentials

test_url = HTTP.URI(config["auth_service"])


HTTP.get(
    config["auth_service"], # Authentication service
    JSON.json(
        Dict(
            "grant_type" => config["grant_type"],
            "refresh_token" => config["refresh_token"],
            "scope" => config["scope"]
        )
    ),
    JSON.json(
        Dict(
            "username" => config["client_id"],
            "password" => config["client_secret"]
        )
    )
)


    status_exception = true # Return status exepection

if response.status_code != 200
        logger.error('Status code: %s')
