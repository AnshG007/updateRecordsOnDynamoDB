About the script :-

The script is used to filter the records from dynamoDB based on the some timestamp and download the PDF into the local folder 
Convert the pdfs into images
The record of the children of the PDF's is stored in Amazon S3 and genertae its URL and soted back to the dynamoDB


How to Start :-

1) Enter into the environment using 
	source <env-name>/bin/activate
	
2) python <file-name>

Steps to Setup the Environment :-

1) sudo yum install python-virtualenv

2) virtualenv <env-name>

3) source <env-name>/bin/activate

Steps to create cronjob :-

What is Cron Job?

A cron job is a Linux command used for scheduling tasks to be executed sometime in the future. This is normally used to schedule a job that is executed periodically


(for ec2 users)

1) Install the package using
	
	sudo yum install cronie
	
2) start the cronjob using

	sudo systemctl start crond
	
3) enable cronjob using

	sudo systemctl enable crond
	
4) Edit or write the cronjob in Vim Editor using

	crontab -e 
	
	(for example create a cronjob that run every 15 minutes of every hour */10 * * * * <absolute path-to python-interpreter> <absolute path-to-python-script>)
	
5) Save the Cronjob file :-

	a) press escape
	b) press shift + :
	c) enter wq
	d) press enter
	
(For Ubuntu users)

1) Edit or write the cronjob in Vim Editor using

	crontab -e 
	
	(for example create a cronjob that run every 15 minutes of every hour */10 * * * * <absolute path-to python-interpreter> <absolute path-to-python-script>)
	
2) Save the Cronjob file :-

	a) press escape
	b) press shift + :
	c) enter wq
	d) press enter
	
