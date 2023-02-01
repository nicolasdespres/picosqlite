# Make a release

1. Choose a version number.

Look at previous release numbers:

```bash
git tag
```

```bash
export VERSION=X.Y.Z
```

2. Write a release note file.

```bash
cat >RelNotes/v$VERSION.txt <<EOF
bla bla bla...
EOF
```

while browsing the change log since last release:

```bash
git log --reverse $(./last-tag.sh)..
```

3. Commit the release note file.

```bash
git add RelNotes/v$VERSION.txt
git commit -m "Add release notes for v$VERSION"
```

4. Create a git tag.

```bash
./tag.sh $VERSION
```

5. Create a release (it is a single python file). The version number
   will be extracted using git-describe(1).

```bash
./release.sh
```

6. Push the commits and the tags.

```bash
git push --tags origin master
unset VERSION
```

7. Create a release on GitHub and drop the file in the dist/ directory
   there.
