# GitHub Flow

🎯 Let's start the day following the **GitHub flow** from the very beginning until deployment. We will deploy a very simple HTML page to focus our learning on `git` and GitHub rather than Python code. Feel free to skip the challenge if you already know how to
- create github repo
- create branch and merge pull requests
- create static github pages


## Some reading

In 2011, Scott Chacon, one of the original founder of GitHub wrote a [blog article](https://scottchacon.com/2011/08/31/github-flow) where he first introduced the concept of Github flow, that he summarizes as follows:
```markdown
- Anything in the master branch is deployable
- To work on something new, create a descriptively named branch from the master (ie: new-oauth2-scopes)
- Commit to that branch locally and regularly push your work to the same named branch on the server
- When you need feedback or help, or you think the branch is ready for merging, open a pull request
- After someone else has reviewed and signed off on the feature, you can merge it into master
- Once it is merged and pushed to `master`, you should deploy immediately
```

💡 Note that the primary branch of most repositories is now named `main` instead of `master`.

## Create repo

Before we actually do our first commit, we need to create a GitHub repository!
Execute the following line one by one, make sure you understand them well!

```bash
mkdir -p ~/code/<user.github_nickname>/github-flow && cd $_

git init # let's start tracking changes in this repo
touch README.md
touch index.html
git add .
git commit -m "Initialize repository"

gh repo create --public --source=.

git push origin main
```

Use `gh browse` you should see the commit and two files!

## Your first Pull Request

Let's start working on this repository. Before touching the code, we must create a **feature branch**. Our goal here is to add a basic HTML skeleton to the project. We can do this:

```bash
git checkout -b html-skeleton
```

This commands _creates_ the branch and _switches_ to it. You can see the Git Bash prompt is updated and no longer displays `main`. You are ready to code in this branch!

Let's open a new VScode at the root of the repo
```bash
code .
```

Open the `index.html` file and write the following HTML code inside

```html
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.1.1/css/bootstrap.min.css" integrity="sha384-WskhaSGFgHYWDcbwN70/dfYBj47jz9qbsMId/iRN3ewGhXQFZCSftd1LZCfmhktB" crossorigin="anonymous">

    <title>Hello, world!</title>
  </head>
  <body>
    <h1>Hello, world!</h1>

    <script src="https://code.jquery.com/jquery-3.3.1.slim.min.js" integrity="sha384-q8i/X+965DzO0rT7abK41JStQIAqVgRVzpbzo5smXKp4YfRvH+8abtTE1Pi6jizo" crossorigin="anonymous"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/popper.js/1.14.3/umd/popper.min.js" integrity="sha384-ZMP7rVo3mIykV+2+9J3UJ46jBk0WLaUAdn689aCwoqbBJiSnjAK/l8WvCWPIPm49" crossorigin="anonymous"></script>
    <script src="https://stackpath.bootstrapcdn.com/bootstrap/4.1.1/js/bootstrap.min.js" integrity="sha384-smHYKdLADwkXOn1EmN1qk/HfnUcbVRZyYmZ4qpPea6sjB/pTJ0euyQp0Mk8ck+5T" crossorigin="anonymous"></script>
  </body>
</html>
```

Let's use python built-in library to serve this HTML in our browser:

```bash
python -m http.server 8000
```

When you are happy with your code and with the way it look in the browser, it's time to commit!

```bash
git status # Which files were changed?
git diff index.html # What changes were done?

git add index.html
git commit -m "Add basic skeleton HTML"
```

Commit is now done locally. Time to push. What will be the command? If you are not sure, `tldr git push` is your best friend!

<details><summary markdown='span'>View solution
</summary>

```bash
git push origin html-skeleton
```
</details>

Now head to your repository on GitHub and refresh the page. You should see this:

![](https://res.cloudinary.com/wagon/image/upload/v1560714729/html-skeleton-pr-suggestion_ilh5o4.png)

Click on the green button to create your first pull request.

## PR Review

You now need someone to look at your code, give some feedback and eventually merge it (a rule when using the GitHub flow is that someone other than the author should merge a Pull Request).

Head over to `github.com/<user.github_nickname>/github-flow/settings/collaboration` (accessible through `Settings` > `Collaborators`) and add your buddy of the day to the repository by asking his/her github nickname. They should receive an email invitation to opt-in.

Once this setup is done, ask them to go to the Pull Request page (should be PR #1) and review the code. If they have some comments (indentation, error, etc.), you need to do some fixes: go back to VS Code, in the same branch, update the code and do another commit. Push this commit to GitHub: you will see that the Pull Request automatically updates!

In the end, if you and your reviewer agree on the code, the reviewer should **merge** the Pull Request. After merging, there is a button "Delete branch". We advise that you click on it, because in the GitHub flow, a **merged branch is a dead branch** and no work should be pushed to it any more.

### What happens next?

Merging the Pull Request on GitHub created a merge commit on `main`. It means that your local repository on your laptop is not up to date any more.

Here is what you need to do:

```bash
# Go back to the branch
git checkout main

# Get your `main` branch up to date with GitHub's
git pull origin main

# The feature branch is dead. Remove it! Keep a clean repo
git branch -d html-skeleton
```

That's it! You are ready to work on the next feature branch Start over at the `git checkout -b <branch>` step.

Let's check our branch network now:

```bash
glog
```
💡 By the way, `glog` is an alias given to you by "oh-my-zsh" terminal plugin. Check out the true command with `which glog`

## Simple Deployment (with Github pages)

If you have a simple **static** website to host, GitHub provides a great solution: [GitHub Pages](https://pages.github.com/). You can turn a repository into a host provider!

It is really simple to enable. On your `github-flow` repository, go to `Settings` tabs then `Pages` on the side navigation menu.

Under the `Source`, click on the dropdown list and select the `main` branch. Then click `Save`

![](https://res.cloudinary.com/wagon/image/upload/v1560714628/enable-github-pages_w5clbv.png)

It will reload the page. If you scroll down you should see a sentence: Your site is ready to be published at:... And here your are! The URL of your website:

```bash
https://<user.github_nickname>.github.io/$REPO_NAME/
```

😎 Every time a commit happens in `main` (through a merged Pull Request using the GitHub flow), GitHub Pages will automatically deploy the changes to this URL. With this set up, the `Merge` button under a Pull Request becomes a **Deploy** one.

If you own a domain, GitHub Pages also supports [`CNAME`](https://help.github.com/articles/using-a-custom-domain-with-github-pages/) configuration.

Github pages can be super useful for holding documentation which is normally all a static site!

## Final thoughts

📚 Github summarized this flow in a [guide](https://guides.github.com/introduction/flow/) that you may want to read now or bookmark for later.

The power of the GitHub flow comes from being accessible even to `git` beginners. `git` is a very powerful tool and can be intimidating if not introduced correctly. With that flow, anyone in the team can pick up the collaboration process with a little training (what you just did!) and just needs to remember a few commands: `status` (and `diff`), `checkout -b`, `add`, `commit -m`, `push`, `checkout`, `pull`, `branch -d` and that's it.

If you talk with other developers about `git`, some advanced concepts might come up, like `stash`, `cherry-pick`, `rebase`, `reset` or `reflog`. There is plenty of time to learn about those topics and adapt your knowledge to your team. We won't cover these topics but at least you have some keywords to Google!
