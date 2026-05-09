# Libs Structure

```text
libs/
├── common/   # shared config and utilities
├── pyshell/  # raw extraction base classes
└── pyspark/  # medallion Spark base classes
```

`common/` must remain generic and reusable across sources.
