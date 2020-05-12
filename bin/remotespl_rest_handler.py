from simple_rest_handler import RestHandler, IntegerFieldValidator, BooleanFieldValidator
import logging
import splunk.admin as admin

class RemoteSPLRestHandler(RestHandler):
    # Below is the name of the conf file (example.conf)
    conf_file = 'remotespl'
    # Below are the list of parameters that are accepted
    PARAM_URI = 'uri'
    PARAM_USERNAME = 'username'
    PARAM_SSLVERIFY = 'sslverify'
    valid_params = [PARAM_URI, PARAM_USERNAME, PARAM_SSLVERIFY]
    required_params = [PARAM_URI, PARAM_USERNAME]
    # List of fields and how they will be validated
    field_validators = {
        PARAM_SSLVERIFY : BooleanFieldValidator()
    }
    app_name = "remotespl"
    logger_file_name = 'remotespl_rest_handler.log'
    logger_name = 'RemoteSPLRestHandler'
    logger_level = logging.DEBUG

if __name__ == "__main__":
    admin.init(RemoteSPLRestHandler, admin.CONTEXT_NONE)
