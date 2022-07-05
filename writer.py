def to_files(df, tgt_dir, file_format):
    df.coalesce(1). \
        write. \
        mode('overwrite'). \
        format(file_format). \
        save(tgt_dir)
