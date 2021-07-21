# Make a release

1. Create a git tag

```bash
./tag.sh <VERSION>
```

2. Create a release (it is a single python file). The version number
   will be extracted using git-describe(1).

```bash
./release.sh
```
