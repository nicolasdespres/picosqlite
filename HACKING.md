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

3. Commit the release note file

```bash
git add RelNotes/vX.Y.Z.txt
git commit -m 'Add release note for vX.Y.Z'
```

4. Create a git tag

```bash
./tag.sh <VERSION>
```

5. Create a release (it is a single python file). The version number
   will be extracted using git-describe(1).

```bash
./release.sh
```

6. Push the commits and the tags.

7. Create a release a GitHub and drop the file in the dist/ directory.
