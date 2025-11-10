# src/myproject/datasets/__init__.py
#
# --- INPUT CONFIG  ---
INPUT_VERSION = "Oct31_weekly"

INPUT_BASE_PATH = "/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark"
OMOP_EAV_DICT_FULL_PATH = f"{INPUT_BASE_PATH}/omop_eav_dict_{INPUT_VERSION}"
OMOP_EAV_DICT_RECORD_FULL_PATH = f"{INPUT_BASE_PATH}/omop_eav_dict_record_{INPUT_VERSION}"


# --- OUTPUT CONFIG ---
OUTPUT_PARENT_DIR = "/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData"
OUTPUT_SUBDIR = "OMOP_spark"

OUTPUT_FULL_BASE_PATH = f"{OUTPUT_PARENT_DIR}/{OUTPUT_SUBDIR}"
