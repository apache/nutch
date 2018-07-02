Thanks for your contribution to [Apache Nutch](http://nutch.apache.org/)! Your help is appreciated!

Before opening the pull request, please verify that
* there is an open issue on the [Nutch issue tracker](https://issues.apache.org/jira/projects/NUTCH) which describes the problem or the improvement. We cannot accept pull requests without an issue because the change wouldn't be listed in the release notes.
* the issue ID (`NUTCH-XXXX`)
  - is referenced in the title of the pull request
  - and placed in front of your commit messages
* commits are squashed into a single one (or few commits for larger changes)
* Java source code follows [Nutch Eclipse Code Formatting rules](https://github.com/apache/nutch/blob/master/eclipse-codeformat.xml)
* Nutch is successfully built and unit tests pass by running `ant clean runtime test`
* there should be no conflicts when merging the pull request branch into the *recent* master branch. If there are conflicts, please try to rebase the pull request branch on top of a freshly pulled master branch.

We will be able to faster integrate your pull request if these conditions are met. If you have any questions how to fix your problem or about using Nutch in general, please sign up for the [Nutch mailing list](http://nutch.apache.org/mailing_lists.html). Thanks!
