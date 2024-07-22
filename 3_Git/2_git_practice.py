# Databricks notebook source
# MAGIC %md
# MAGIC ### Git in Databricks is called Databricks Repos.
# MAGIC - Interactive GUI to work with Git providers like GitHub
# MAGIC - Support of multiple file types and python modules not only notebooks

# COMMAND ----------

# MAGIC %md
# MAGIC ### GitHub Repository
# MAGIC A repository is usually used to organize a single project. Repositories can contain folders and files, images, videos, spreadsheets, and data sets -- anything your project needs. Often, repositories include a README file, a file with information about your project.
# MAGIC
# MAGIC A repository is a unit in which Git version control operates.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Git as a post service 
# MAGIC When working with a git you can imagine a real-life postal service. All the operations we will be doing will resemble a postal operation. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clone your first repository
# MAGIC Cloning a repository resembles either arriving at the postal service to packaging department. 
# MAGIC To clone your first repository:
# MAGIC 1. Go to https://github.com/DataSentics/git-practice
# MAGIC 2. Click Clone
# MAGIC 3. Click Https
# MAGIC 4. Copy the URL
# MAGIC ![github-clone-repo](https://github.com/DataSentics/odap-workshops/blob/main/Git/images/clone-repo.png?raw=true)
# MAGIC 5. Go back to Databricks
# MAGIC 6. Click Repos
# MAGIC 7. Click Add Repo
# MAGIC 8. Enter the URL
# MAGIC ![github-clone-repo](https://github.com/DataSentics/odap-workshops/blob/main/Git/images/add-repo-1.png?raw=true)
# MAGIC ![github-clone-repo](https://github.com/DataSentics/odap-workshops/blob/main/Git/images/add-repo-2.png?raw=true)
# MAGIC
# MAGIC **Difference between HTTPS and SSH authentication**
# MAGIC - HTTPS is based on user-name and password or PAT tokens. Used for interaction with Git from Databricks.
# MAGIC - SSH is based on private/public key pair. Used usually when interacting with git from your local machine.
# MAGIC
# MAGIC In CLI you would do ```git clone https://repo-url.com```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Git branches
# MAGIC When working with git you are always developing on a branch. Git branch represents an independent line of development. When working on a new feature or a bug-fix we want to keep track of the different changes we are making separately. Once we are done with our work we want to include these changes into the original repository. 
# MAGIC
# MAGIC Creating a new branch resembles creating a new package in postal service with already prepared content. 
# MAGIC
# MAGIC **Main branch**
# MAGIC
# MAGIC In the repository there should always be one main branch which contains the production. This branch is kept protected and no one can push into it directly. Putting changes into this branch is done through pull-requests which will be explained later. The number of protected branches can be higher. For example one branch for development one for testing and one for production.
# MAGIC
# MAGIC #### To create a new branch for your development:
# MAGIC 1. Click Repos
# MAGIC 2. Click your current repository
# MAGIC ![git-branch-1](https://github.com/DataSentics/odap-workshops/blob/main/Git/images/git-branch-1.png?raw=true)
# MAGIC 3. Click create branch
# MAGIC ![git-branch-1](https://github.com/DataSentics/odap-workshops/blob/main/Git/images/git-branch-2.png?raw=true)
# MAGIC 4. Enter your branch name. In the branch name should be specified to whom the branch belongs and what does it contain. Eg. tbo-person-age-calculation. Based-on tells you on which branch your current branch will be based on. 
# MAGIC 5. Click Create
# MAGIC ![git-branch-1](https://github.com/DataSentics/odap-workshops/blob/main/Git/images/git-branch-3.png?raw=true)
# MAGIC Now you created a new branch and automatically switched your context to it
# MAGIC
# MAGIC In git CLI to create a new branch: ```git checkout -b "branch-name"```

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### To switch to a different branch
# MAGIC 1. Click Repos
# MAGIC 2. Click your current repository
# MAGIC ![git-branch-1](https://github.com/DataSentics/odap-workshops/blob/main/Git/images/git-branch-1.png?raw=true)
# MAGIC 3. Click your current branch name
# MAGIC 4. Select desired branch from drop-down
# MAGIC ![git-branch-switch](https://github.com/DataSentics/odap-workshops/blob/main/Git/images/switch-branch.png?raw=true)
# MAGIC
# MAGIC
# MAGIC Now you switched your context to the desired branch. Look for any files that changed
# MAGIC
# MAGIC In git CLI to switch to existing branch ```git branch branch-name```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Make a change
# MAGIC Now it is time to make some changes. This change can resemble creating a new item which you will want to insert into your box. 
# MAGIC
# MAGIC **TODO**
# MAGIC Create a new text file "your-name.txt" that will contain your favorite qoute. If you do not have a favorite qoute you can insert “Programmer: A machine that turns coffee into code."

# COMMAND ----------

# MAGIC %md
# MAGIC ### Git add
# MAGIC Once you are done you need to incorporate this change to your repository. This resembles adding your item into the box. To do this:
# MAGIC
# MAGIC In command line interface you would do ```git add file-name.py```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Git commit
# MAGIC Once you added your changes with ```git add``` you need to commit them. Commiting resembles packing an item into your box and putting a comment onto it. It is a separate subset of work in given branch. Based on commits Git keeps the history. 
# MAGIC
# MAGIC In CLI you can do ```git commit -m "Message describing the commit"```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Git push
# MAGIC Once you committed your changes you need to push them. Until now your changes has been kept in your local working environment. Once you push the changes will be available to everyone with access to the repository. The changes will be still kept in the separate branch. Pushing changes resembles putting address onto your box and sending it. 
# MAGIC
# MAGIC In CLI you can do ```git push origin branch-name```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Git add, commit and push in Databricks
# MAGIC 1. Click Repos
# MAGIC 2. Click your repository
# MAGIC ![git-branch-1](https://github.com/DataSentics/odap-workshops/blob/main/Git/images/git-branch-1.png?raw=true)
# MAGIC 3. Check your changes. Keep the changes you are satisfied with and discard any changes you are not satisfied with.
# MAGIC 4. Write your commit message describing the changes you made
# MAGIC 5. Click commit and push
# MAGIC ![git-changes](https://github.com/DataSentics/odap-workshops/blob/main/Git/images/git-add-commit-push.png?raw=true)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create pull request
# MAGIC Now that you have pushed your changes, you want to include them into the main branch. To include it into protected main branch you need create a pull-request. The process of creating a pull request resembles a proces of controlling the package in postal service before delivering it to client(in our case production). 
# MAGIC
# MAGIC **To create a pull-request:**
# MAGIC 1. Go to your repository on github.com
# MAGIC 2. Click pull-requests
# MAGIC 3. Click new pull-request
# MAGIC ![create-pr1](https://github.com/DataSentics/odap-workshops/blob/main/Git/images/git-create-pr-1.png?raw=true)
# MAGIC 4. Select from which branch the new pull request will be created
# MAGIC 5. Click create-pull-request
# MAGIC ![create-pr1](https://github.com/DataSentics/odap-workshops/blob/main/Git/images/git-create-pr-2.png?raw=true)
# MAGIC 6. Once a pull-request has been created you need to ask someone to review your changes. To do so you can just send him the link to your pull-request. He can either request changes(add comments) to your pull-request or approve them.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Git pull
# MAGIC Overtime other poeple make changes to their respective branches and new branches are created. Also changes in the main branch happen. To get all the new changes from other people you need to pull them. The pulling happens for each branch separately. 
# MAGIC
# MAGIC The already made changes may be conflicting with your local changes. Therefore make sure you pull often so you do not encounter problems. 
# MAGIC
# MAGIC To perform a pull 
# MAGIC
# MAGIC In CLI you can do ```git pull origin branch-name```
# MAGIC
# MAGIC 1. Click Repos
# MAGIC 2. Click your repository
# MAGIC 3. Click pull
# MAGIC
# MAGIC ![git-pull](https://github.com/DataSentics/odap-workshops/blob/main/Git/images/git-pull.png?raw=true)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Practice time
# MAGIC Your colleague went for a holiady but the task he was working on needs to be finished until tomorrow. Luckily he pushed his work to GitHub before leaving. 
# MAGIC **TODO**
# MAGIC 1. Pull the work of your colleague. The branch in which is the work is called ```tbo-wrong-table-name-bug-fix``` in repository git-practice.
# MAGIC 2. Create your own branch based on the branch of your colleague.
# MAGIC 3. Make the necessary changes to fix the bug. To reveal the bug look for notebook in the file repository and try to run it. 
# MAGIC 4. Create a pull-request.
# MAGIC 5. Make sure to get a code review.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conflicts in git pull requests
# MAGIC When you made certain changes to a file and someone else also made changes to the same file git can usually take care of it. But sometimes it happen that git is not able to choose which changes to keep and therefore creates conflicts in your pull-request. 
# MAGIC
# MAGIC An example can be seen if we try to create pull-request in repository ```git-practice``` from branch ```tbo-new-qoute``` to ```main```.
# MAGIC
# MAGIC ![git-conflict-1](https://github.com/DataSentics/odap-workshops/blob/main/Git/images/git-conflict-1.png?raw=true)
# MAGIC
# MAGIC To resolve the conflicts we locate resolve-conflicts button and click it
# MAGIC
# MAGIC ![git-conflict-2](https://github.com/DataSentics/odap-workshops/blob/main/Git/images/git-conflict-2.png?raw=true)
# MAGIC
# MAGIC Than we are met with the following code:
# MAGIC
# MAGIC ```
# MAGIC <<<<<<< tbo-new-qoute
# MAGIC Changes I made to this file
# MAGIC =======
# MAGIC Changes someone else made to a file.
# MAGIC >>>>>>> main
# MAGIC ```
# MAGIC
# MAGIC ![git-conflict-3](https://github.com/DataSentics/odap-workshops/blob/main/Git/images/git-conflict-3.png?raw=true)
# MAGIC
# MAGIC To fix the conflict we need to delete one of the changes that has been made. We need to understand the code and choose the correct change. In this case we want to keep the change that has been made in ```tbo-new-qoute``` branch. To keep the change we just delete everything else until we are met with following:
# MAGIC
# MAGIC ```Changes I made to this file```
# MAGIC
# MAGIC Than we just click Mark as resolved.
# MAGIC
# MAGIC ![git-conflict-4](https://github.com/DataSentics/odap-workshops/blob/main/Git/images/git-conflict-4.png?raw=true)
# MAGIC
# MAGIC And than Commit Merge
# MAGIC
# MAGIC ![git-conflict-5](https://github.com/DataSentics/odap-workshops/blob/main/Git/images/git-conflict-5.png?raw=true)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pull fails
# MAGIC Sometimes when two of you are working on the same branch it might happen that your colleague pushed some changes on to the branch. Than you make some changes on the same branch. Than you want to pull the changes of your colleague. This will result in a conflict that requires special solving. **This is why you always try to keep the branches just for single person**.
# MAGIC
# MAGIC 1. Change your branch to ```cs-pull-conflict-example```
# MAGIC 2. Make some changes to conflict.txt
# MAGIC 3. Give Toso or Lukas some time to push into the  ```cs-pull-conflict-example```
# MAGIC 4. Try to pull the changes. A window will pop-up showing you and error and requiring you to create pull-request to fix the conflicts
# MAGIC 5. Click Resolve conflict using PR
# MAGIC ![git-pull-conflict-1](https://github.com/DataSentics/odap-workshops/blob/main/Git/images/git-pull-conflict-1.png?raw=true)
# MAGIC 6. Enter branch name into Branch and click commint to new branch
# MAGIC ![git-pull-conflict-2](https://github.com/DataSentics/odap-workshops/blob/main/Git/images/git-pull-conflict-2.png?raw=true)
# MAGIC 7. Create a pull request from the new branch into the old one
# MAGIC ![git-pull-conflict-3](https://github.com/DataSentics/odap-workshops/blob/main/Git/images/git-pull-conflict-3.png?raw=true)
# MAGIC 8. Follow the same steps as when fixing conflicts from above

# COMMAND ----------

# MAGIC %md
# MAGIC ### Interesting (advanced) topics for self-study.
# MAGIC - .gitignore
# MAGIC - Create a new repo
# MAGIC - Git Graph
# MAGIC - GitHub Actions
# MAGIC - Continuos Integration(CI)/Continuos Delivery(CD) on GitHub
