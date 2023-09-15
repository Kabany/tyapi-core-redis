# Contributing to TyAPI-Core-MySQL

We would love for you to contribute to TyAPI-Core-MySQL and help make it even better than it is today! As a contributor, here are the guidelines we would like you to follow:

## Question or Problems

If you have a question about TyAPI-Core-MySQL (not a bug report) please post it to [StackOverflow][stackoverflow] with the tag `tyapi-core-redis`.

## Security vulnerabilities

If you find a security vulnerability or something that should be discussed personally, please contact us within our [email](mailto:developers@kabany.mx?subject=Security Vulnerability Report).

## Reporting bugs

If you find a bug in the source code, you can help us by [submitting an issue](#issue) to our [GitHub Repository][github-repository]. Even better, you can submit a [Pull Request](#pull-request) with a fix. Remember that before opening a new issue, look for existing issues to avoid duplication.

## New Features

You can request a new feature by [submitting an issue](#issue) to our [GitHub Repository][github-repository]. Just remeber before opening a new issue, look for existing issues to avoid duplication.

## Submitting Guidelines

### <a name="issue"></a> Submit an issue

Before you submit an issue, please search the [issue tracker][github-issue], maybe an issue for your problem already exists and the discussion might inform you of workarounds readily available.

We want to fix all the issues as soon as possible, but before fixing a bug we need to reproduce and confirm it. In order to reproduce bugs we ask you to provide a minimal code snippet that shows a reproduction of the problem.

You can file new issues by filling out a [new issue form][github-issue-new].

### <a name="pull-request"></a> Submit a Pull Request

Before you submit your Pull Request consider the following guidelines:

* Before you start to write code, look for existing [issues][github-issue]. That way you avoid working on something that has been addressed already in a different branch. You can create a new issue [here][github-issue-new].
* Also check for an open or closed [Pull Request][github-pull] that relates to your submission. You don't want to duplicate effort.
* The source of this project is written in TypeScript.
* Make your changes in a new git branch:
  ```shell
  git checkout -b your-fix-branch master
  ```
* Create your patch, including appropriate test cases. Without tests your Pull Request will not be accepted.
* Make sure all the previous tests still pass:
  ```shell
  npm install
  npm run test
  ```
* Commit your changes using a descriptive commit message that follows our [commit message conventions](#commit).
* Push your branch to GitHub:
  ```shell
  git push origin your-fix-branch
  ```
* In GitHub, send a pull request to `main`.
* If we suggest changes then:
  - Make the required updates.
  - Re-run the tests to ensure tests are still passing.
  - Rebase your branch and force push to your GitHub repository (this will update your Pull Request):
    ```shell
    git rebase master -i
    git push -f
    ```
* And that's it! Thank you for your contribution!

#### After your pull request is merged

After your pull request is merged, you can safely delete your branch and pull the changes from the main (upstream) repository:

* Delete the remote branch on GitHub either through the GitHub web UI or your local shell as follows:
  ```shell
  git push origin --delete your-fix-branch
  ```
* Check out the master branch:
  ```shell
  git checkout master -f
  ```
* Delete the local branch:
  ```shell
  git branch -D your-fix-branch
  ```
* Update your master with the latest upstream version:
  ```shell
  git pull --ff upstream master
  ```

## Commit Message Guidelines

We recommend you to follow the rules over how our git commit messages can be formatted. This leads to more readable messages that are easy to follow when looking through the project history. We follow the conventions explained in [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/).

### Commit Message Format

Each commit message consists of a **header**, a **body** and a **footer**. The header has a special format that includes a **type** and a **subject**:

```
<type>: <description>

[optional body]

[optional footer(s)]
```

Any line of the commit message cannot be longer 100 characters! This allows the message to be easier to read on GitHub as well as in various git tools.

### Revert
If the commit reverts a previous commit, it should begin with `revert: `, followed by the header of
the reverted commit. In the body it should say: `This reverts commit <hash>.`, where the hash is
the SHA of the commit being reverted.

### Type
Must be one of the following:

* **feat**: A new feature
* **fix**: A bug fix
* **docs**: Documentation only changes
* **style**: Changes that do not affect the meaning of the code (white-space, formatting, missing semi-colons, etc)
* **refactor**: A code change that neither fixes a bug nor adds a feature
* **perf**: A code change that improves performance
* **test**: Adding missing tests or correcting existing tests
* **build**: Changes that affect the build system, CI configuration or external dependencies
* **chore**: Other changes that don't modify `src` or `test` files

### Subject
The subject contains succinct description of the change:

* use the imperative, present tense: "change" not "changed" nor "changes"
* don't capitalize first letter
* no dot (.) at the end

### Body
Just as in the **subject**, use the imperative, present tense: "change" not "changed" nor "changes".
The body should include the motivation for the change and contrast this with previous behavior.

### Footer
The footer should contain any information about **Breaking Changes** and is also the place to
reference GitHub issues that this commit **Closes**.

**Breaking Changes** should start with the word `BREAKING CHANGE:` with a space or two newlines.
The rest of the commit message is then used for this.

### Examples
Fix and close issue:
```
fix: resolve issues uppercase column names

Closes: #123456
```
Implement new feature:
```
feat: implement new magic decorator

This new feature change bahviour of typeorm to allow use new magic decorator...

Closes: #22222
```
Docs update:
```
docs: update supported mssql column types
```
Breaking change:
```
refactor: refactor driver API

BREAKING CHANGE: description of breaking change in driver API
```



<!-- Markdown link & img dfn's -->
[stackoverflow]: https://stackoverflow.com/questions/tagged/tyapi-core-redis
[github-repository]: https://github.com/Kabany/tyapi-core-redis
[github-issue]: https://github.com/Kabany/tyapi-core-redis/issues
[github-issue-new]: https://github.com/Kabany/tyapi-core-redis/issues/new
[github-pull]: https://github.com/Kabany/tyapi-core-redis/pulls