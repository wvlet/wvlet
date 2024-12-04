# Release

Wvlet follows `(year).(milestone month).(patch)` versioning scheme, e.g., 2024.9.0, 2024.12.0, ...
See [#170](https://github.com/wvlet/wvlet/issues/170) for the rationale. 

To create a new release, run `./project/release.rb` script at the main branch:
```bash
$ git checkout main
$ ./project/release.rb
```

This will add a new git tag, and GitHub Action will create a new release note and tar.gz archive automatically. 

After releasing a new version and if it's stable enough, you can also distribute the release for Homebrew: https://github.com/wvlet/homebrew-wvlet
