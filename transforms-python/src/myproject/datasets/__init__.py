# src/myproject/datasets/__init__.py

# --- INPUT CONFIG  ---
INPUT_BASE_PATH = "/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark"
INPUT_VERSION = "Oct03_weekly"
# This is the complete input path you'll import
OMOP_EAV_DICT_FULL_PATH = f"{INPUT_BASE_PATH}/omop_eav_dict_{INPUT_VERSION}"


# --- OUTPUT CONFIG ---
OUTPUT_PARENT_DIR = "/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData"
OUTPUT_SUBDIR = "OMOP_spark"
# This new variable combines the two above for easy use
OUTPUT_FULL_BASE_PATH = f"{OUTPUT_PARENT_DIR}/{OUTPUT_SUBDIR}"