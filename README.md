# metaDataBus-sample-java
This project contains sample code for Notifications sending via SMS. It works by first processing numbers/provided data from a file and adding them into a database table. 
Then a scheduler executes to process all valid entries(Based on time) from the said table and add them for SMS sending to another table. A second scheduler executes and send the notifications and add them in the last table to maintain history.
