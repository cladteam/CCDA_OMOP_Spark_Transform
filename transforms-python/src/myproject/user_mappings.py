from numpy import int32
from numpy import float32
import prototype_2.value_transformations as VT

# To create local mappings overlay one or more of the configs here.
# There is a commented-out and abbreviated copy of a Measurements config here.
# The block of setting config maps to None is an easy way to test this.
# You won't get much back without those configs.
#
# Note that you may have to import libraries as in the original metadata files.
#
# You should see the message "iNFO: got user mappings  and overlaid them." 
#  if your overlay mappings are properly loaded.

overlay_mappings = {
    'Measurement_results': None,
    'Measurement_vital_signs': None,
    'Observation': None,
    'Procedure_activity_act': None,
    'Procedure_activity_procedure': None,
    'Procedure_activity_observation': None,
    'Condition': None,
    'Medication_medication_activity': None,
    'Medication_medication_dispense': None,
    'Immunization_immunization_activity': None
    

# REMOVE THIS ENTRY PRIOR TO DEVELOPMENT WORK, replace with your own
    #### start Measurement_results config

#    'Measurement_results': {
#    	'root': {
#    	    'config_type': 'ROOT',
#            'expected_domain_id': 'Measurement',
#    	    'element':
#    		  ("./hl7:component/hl7:structuredBody/hl7:component/hl7:section/"
#    		   "hl7:templateId[@root='2.16.840.1.113883.10.20.22.2.3.1' or @root='2.16.840.1.113883.10.20.22.2.3']"
#    		   "/../hl7:entry/hl7:organizer/hl7:component/hl7:observation")
#        },
#        
#    	'measurement_id_root': {
#            'config_type': 'FIELD',
#            'element': 'hl7:id[not(@nullFlavor="UNK")]',
#            'attribute': 'root'
#    	},
#    	'measurement_id_extension': {
#            'config_type': 'FIELD',
#            'element': 'hl7:id[not(@nullFlavor="UNK")]',
#            'attribute': 'extension'
#    	},
#    	'measurement_id': {
#    	    'config_type': 'HASH',
#            'fields' : ['person_id',  'provider_id',
#						#'visit_occurrence_id',
#						'measurement_concept_code', 'measurement_concept_codeSystem',
#						'measurement_date', 'measurement_datetime',
#                        'value_as_number', 'value_as_concept_id',
#				        'measurement_id_root', 'measurement_id_extension'],
#            'order': 1
#    	}
#    }
    #### end Measurement_results config

} # end overlay_mappings

