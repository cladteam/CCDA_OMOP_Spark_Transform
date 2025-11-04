# SQL subproject

You can create SQL transformations by creating a file ending in `.sql` under this subproject's `src/main/sql` subdirectory.

An example of a SQL transformation is--

```
CREATE TABLE `/path/to/example` AS
    SELECT * FROM `/path/to/input/dataset`
```

If you need to temporarily ignore a transformation from the CI build process, you can rename the file to use an
extension other than `.sql`.