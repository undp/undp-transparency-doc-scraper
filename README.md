# UNDP Transparency Files

## PDC Document Scraper
The document scraper is intended to pull documents from pdc and categorise them based on content. The logic is defind inside the files. The scraper is to be run monthly.

The scraper can be executed via Jupyter Notebook final_document_scraper.ipynb. In this case the first two cells need to be run, then the kernel needs to be reset. Then cells 1 and 3-15 need to be executed. The notebook will generate temporary files to analyse the documents and update the file documents.csv which must be in the same directory.

documents.csv must then be manually uploaded to the Transparency folder on Sharepoint.

The alternative is to run the two python scripts new_scraper_p1.py and new_scraper_p2.py after each other. A similar update will be performed to documents.csv.

## PWYF replica

A replica of PWYF is instalaled on Azure server 20.124.5.43. 

Notes: Due to storage restraints only some organisations can be updated at a time.

## OECD sector review

OECD Sector Review 2.ipynb (the most recent version) uses the DAC-CRS-CODES.xlsx file as an input to categorise titles and descriptions to sector codes. It then analyses a set of projects and categorises them based on a matching algorithm. 
