################################################################################
# PREPARE WORKSPACE #
################################################################################

########################################
# LOAD PACKAGES #
########################################
using JSON # Parsing and printing JSON files
using Base64 # Functionality for base-64 encoded strings and IO
using DelimitedFiles # Parsing and printing various delimited file formats
using ODBC # Interface for the ODBC API
using Tables # Interface for tabular data
using TableOperations # Common table operations on Tables.jl compatible sources
using HTTP # HTTP client and server functionality

########################################
# LOAD CONFIGURATION FILE #
########################################
cd("")
credentials = JSON.parsefile("credentials.json")

########################################
# SPECIFY DATASETS #
########################################

####################
# FULL DATASETS #
####################
FULL_DATASETS_META = Dict(
    "org_units" => "07a9e561-e22f-4e82-8dd6-7bfb14c91776'",
    "grade_objects" => "793668a8-2c58-4e5e-b263-412d28d5703f",
    "users" => "1d6d722e-b572-456f-97c1-d526570daa6b",
    "grade_results" => "9d8a96b4-8145-416d-bd18-11402bc58f8d",
    "user_enrollments" => "533f84c8-b2ad-4688-94dc-c839952e9c4f"
)

####################
# DIFFERENTIAL DATASETS #
####################
DIFF_DATASETS_META = Dict(
    "user_enrollments" => "a78735f2-7210-4a57-aac1-e0f6bd714349"
)

################################################################################
# TRADE IN REFRESH TOKEN #
################################################################################





plaintext = config["client_id"] * ":" * config["client_secret"]

io = IOBuffer();
iob64_encode = Base64EncodePipe(io);
write(iob64_encode, plaintext);
close(iob64_encode);
encoded = String(take!(io))

value = "Basic " * encoded

params = Dict("user"=>"RAO...tjN", "token"=>"NzU...Wnp", "message"=>"Hello!")
base_url = "http://api.domain.com"
endpoint = "/1/messages.json"
url = base_url * endpoint
r = HTTP.request("POST", url,
            ["Authorization" => value],
             JSON.json(params))
println(JSON.parse(String(r.body)))
