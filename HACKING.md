# Make a release

1. Choose a version number

Look at previous release numbers:

```bash
git tag
```

2. Write a release note file

```bash
cat >RelNotes/vX.Y.Z.txt <<EOF
bla bla bla...
EOF
```

while browsing the change log since last release:

```bash
git log vX.Y.Z..
```

3. Create a git tag

```bash
./tag.sh <VERSION>
```

4. Create a release (it is a single python file). The version number
   will be extracted using git-describe(1).

```bash
./release.sh
```
