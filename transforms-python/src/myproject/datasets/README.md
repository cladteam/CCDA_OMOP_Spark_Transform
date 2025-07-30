README.md

# Building  CCDA OMOP Spark Transform / transforms-python / src / myproject / datasets

- start with **omop_eav_dict**, details below. It genertes a file calle omop_eav_dict, sometimes with a date suffix used by the next steps. It's done this way because Spark, AFAIK, just allows a single output table/file.
- The OMOP tables, could call it **stage 0**,  in the top-level of the datasets folder, reads the omop_eav_dict and creates OMOP tables. 
- Note that part of the build process invovles renaming files, and you should coordinate that in this script.
- **uniquify_stage_1** runs drop_duplicates
- **post_vocab_stage_2** is there to use the crosswalk maps to do concept mapping because the thing that generates omop_eav_dict isn't working right and do it there.

Further processing happens in other pipelines to bring this data into the data counts project, and to copy the DQ tables into a place where the permissions are better. See the monocle for the DQ stuff. Data counts may not be there yet.

## Running builds of omop_eav_dict on the entire set
At writing, there are over 50k input files and just running a a single copy of omop_eav_dict.py would take a week or more. To aleviate this, it's written using the flatMap funcion on rdd that distributes processing among a number of machines simultaneously. this is configured using the line of code copied below:

```@configure(profile=['DRIVER_MEMORY_EXTRA_LARGE', 'EXECUTOR_MEMORY_LARGE', 'NUM_EXECUTORS_64'])```

You could simply run the code this way and it would run for something like two days. The risk is that it fails part way along and needs to be restarted, and you'd have to re-run many hours of previous computation. To alleviate that, incremantal builds are set up. This involves two parts of the code, pasted below.

```
@incremental(semantic_version=1, snapshot_inputs=["input_files"] )
@configure(profile=['DRIVER_MEMORY_EXTRA_LARGE', 'EXECUTOR_MEMORY_LARGE', 'NUM_EXECUTORS_64'])
@transform(
    omop_eav_dict = Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/omop_eav_dict_May21"),
    previous_files = Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/omop_eav_dict_May21_record"),
```
- First is the @incremental tag
- Second is the  extra output, omop_eav_dict_May21_record that this process uses to keep track of how far along it is. 
- Third is the constant STEP_SIZE which tells how many input files to run per batch. As I recall 1000 files runs in something like 20 minutes.

To make this go:
- [ ] rename the output and record files in the omop_eav_dict.py file. Running again with May21 won't do anything because the record says it's finished already.
- [ ] start the first batch by "build"ing the omop_eav_dict.py file.
- [ ] use the build menu, hammer icon, to set up a recurring schedule on omop_eav_dict.py to repeat every 20 minutes or so. If the time increment is too small, it will wait for the current batch to finish. If it's too big, it won't start the next batch until that amount of time has passed even it the current batch has finished. I haven't tried setting it to 5 minutes....

### Detail of Setting a Schedule

## Running builds on the first 1000 files.
This is easy, just run the code after modifying the filenames and don't  set up a repeating build.


## Running Builds on 1000 files from deeper in the retrieved set. 
TBD. 

## If in development mode, please do this in a branch or change the name of the file. 
The master datasets may be in use by people working on DQ or something and an incomplete set may be inconvenient for them.
As I recall there are steps with the other pipelines that will only read from master, so it doesn't always work to use a branch. Changing the name of the omop_eav_file protects the investment of time building that. It doesn't solve the branch problem for the OMOP tables though. TBD
