from transforms.api import Input, Output, transform, configure, incremental

STEP_SIZE = 2000
# cribbed from A. Bailey https://foundry.cladplatform.org/workspace/code/repos/ri.stemma.main.repository.a85503dc-1783-4a19-b714-7ba3140e0879/contents/refs%2Fheads%2Fmaster/transforms-python/src/myproject/datasets/ccda_rd_responses.py

#@incremental( semantic_version=1, snapshot_inputs=["input_files"], )
#@configure(profile=['DRIVER_MEMORY_LARGE', 'EXECUTOR_MEMORY_LARGE', 'NUM_EXECUTORS_16'])
#@transform(
#    output_files=Output("ri.foundry.main.dataset.8c8ff8f9-d429-4396-baed-a3de9c945f49"),
#    previous_files_record=Output("ri.foundry.main.dataset.fb2c61e1-4a87-4d24-bf70-9f8e8384aada"),
#    input_files=Input("ri.foundry.main.dataset.0a1bf8fb-776d-42d7-864d-d979e5c7457e"),
#)
def compute(ctx, output_files, previous_files_record, input_files):

    # KILLSWITCH
    # This prevents us from ever accidentally snapshotting and losing the data built so far if not running incrementally.
    if not ctx.is_incremental:
        output_files.abort()
        previous_files_record.abort()
        raise Exception("not incremental build, self destructing")
        return

    # Set output files dataset to append rather than snapshot
    output_files.set_mode("modify")

    input_fs = input_files.filesystem()
    input_files_df = input_fs.files()

    # exclude what we've already copied : left antijoin
    previous_files_df = previous_files_record.dataframe(mode="previous")
    input_files_df = input_files_df.join(previous_files_df, ['path'], 'leftanti')
    # limit to another STEP_SIZE sized batch
    input_files_df = input_files_df.limit(STEP_SIZE).checkpoint(eager=True)


    # Process the input files as needed
    input_files_files_list = input_files_df.collect()
    for file_row in input_files_files_list:
        # Read the file content
        with input_fs.open(file_row.path, 'rb') as f:
            content = f.read()
            # Write the processed content to the output filesystem
            output_fs = output_files.filesystem()
            with output_fs.open(file_row.path, 'wb') as out_f:
                out_f.write(content)



    # Append rows of documents written for record

    # Q: why do we not have to set previous_files to modify like we did for output file.
    previous_files_record.write_dataframe(input_files_df)