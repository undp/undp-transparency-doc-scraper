#need to sudo apt-get install tesseract-ocr
#need to also install poppler-utils
from PIL import Image 
#import pytesseract 
import sys 
from pdf2image import convert_from_path 
import os 
import pandas as pd, math, inspect, requests
from lxml import html
import re, json, io
import textract
import urllib.parse
import scrapy
from scrapy.crawler import CrawlerProcess
from PyPDF2 import PdfFileReader
import os
import urllib
import ssl
import pdfpages
import pikepdf
#from docx2pdf import convert
from datetime import date
today = date.today().strftime("%Y-%m-%d")
import pdfplumber

#Document List scraper. 
#Run after and independently of document folder scraper

#read existing document data
dfcsv = pd.read_csv('documents.csv')
dfcsv['doctype']=dfcsv['url'].str.split('.').str[-1].str.lower()
dfcsv = dfcsv[dfcsv['filename'].str.contains('00132553')==False]

#document list scraper
#get folders to scrape
dfolders = pd.read_csv('temp-documentfolders.csv')
os.remove("temp-documentfolders.csv")    #<-------------------------------<<<<------
folder_list = dfolders['url'].tolist()

class BootstrapTableSpider2(scrapy.Spider):
    name = "un_test"
    custom_settings = {
        'FEED_FORMAT': 'csv',
        'FEED_URI': 'temp-documents.csv'
    }

        
    def start_requests(self):
        
        urls = folder_list
        for url in urls:
            yield scrapy.Request(url, self.parse)

    
    def parse(self, response):
        rows = response.css('.ms-itmhover')
    
        for row in rows:
            if row.css('.ms-vb2')[5].css('td::text').extract_first()== 'Prodoc':
                row_id = row.css('.ms-vb').css('div').xpath('@id').extract_first()
                row_document_id = row.css('.ms-vb2')[6].css('a::text').extract_first()
                
                #if (dfcsv[dfcsv['id'].isin([row_id])].empty == True) & (dfcsv[dfcsv['document_id'].isin([row_document_id])].empty == True): 
                yield {
                      'filename' :  row.css('.ms-vb').css('a::text').extract_first(),
                      'title' : row.css('.ms-vb2')[0].css('td::text').extract_first(),
                      'id' : row.css('.ms-vb').css('div').xpath('@id').extract_first(),
                      'document_id' : row.css('.ms-vb2')[6].css('a::text').extract_first(),
                      'project_id' : row.css('.ms-vb2')[3].css('td::text').extract_first(),
                      'document_category' : row.css('.ms-vb2')[4].css('td::text').extract_first(),
                      'pdc_document_type' : row.css('.ms-vb2')[5].css('td::text').extract_first(),
                      'modified' : row.css('.ms-vb2')[7].css('nobr::text').extract_first(),
                      'created' : row.css('.ms-vb2')[8].css('nobr::text').extract_first(),
                      'operating_unit' : row.css('.ms-vb2')[2].css('td::text').extract_first(),
                      'url' : 'https://info.undp.org' + row.css('.ms-vb').css('a')[0].xpath('@href').extract_first(),
                }

try:
    os.remove("temp-documents.csv")
except:
    pass



#%%capture
process = CrawlerProcess();
process.crawl(BootstrapTableSpider2);
process.start();

#read new document data and append to existing
try:
    dfcsv_newdocs = pd.read_csv('temp-documents.csv')
    os.remove("temp-documents.csv")
    dfcsv_newdocs['doctype']=dfcsv_newdocs['url'].str.split('.').str[-1].str.lower()
    newdoc = True
except:
    newdoc = False

#dfcsv.loc[dfcsv['document_id'].isin(dfcsv_newdocs['document_id'])==False,'eval2']="fixed"
#dfcsv.loc[dfcsv['document_id'].isin(dfcsv_newdocs['document_id'])==False,'eval1']="fixed"
#dfcsv.loc[dfcsv['document_id'].isin(dfcsv_newdocs['document_id'])==False,'last_update']=today

dfcsv_newdocs['last_update'] = today

if newdoc == True:
    dfcsv = dfcsv.append(dfcsv_newdocs[dfcsv_newdocs['document_id'].isin(dfcsv['document_id'])==False],ignore_index=True)

#Score Criteria
#Score Keywords:
#Good if scroe >=4
project_title = ['project title','title of the program','title of program', 'award title', 'de projet', 'titulo proyecto', 'titulo de proyecto', 'título proyecto', 'de proyecto','titulo del proyecto', 'título del proyecto']
summary_desc = ['summary', 'project description', 'brief description','breve description', 'breve descripcion', 'breve descripción', 'resume de projet','resume', 'résumé', 'resumen']
project_id = ['award id', 'project no','project number','numero de projet','identifiant','du projet','numero proyecto','projecto id','número del proyecto','numero del proyecto']
implementing_partner = ['implementing partner', 'executing partner', 'implementing organization','implementing un organization', 'implementing organisation', 'implementing agenc', 'executing agenc', 'partenaire dexecution', 'partenaire d’exécution' ,'agence dexecution', 'agence d’exécution', 'agencia ejecutora', 'asociado en la implementación']
start_date = ['commencing date','start date', 'commencement date', 'starting date','start year','duration of program', 'period,duration', 'date de debut', 'date de début','periode', 'periodo','fecha de inicio', 'ano inicio' ]
end_date = ['ending date','end date', 'period end date', 'end year', 'project period','duration of program','project duration', 'date de fin', 'fin de periode', 'fin de période', 'fecha final', 'ano finalizacion','fecha de finalización','fecha de finalizacion']
budget = ['total budget','total resources', 'total fund', 'required budget', 'estimated resources', 'resources required', 'total','financial','budget requis', 'total financiamiento', 'total financiero','presupuesto']

#additional metrics to determine if document is "OK"
#If the document quality is good but the total page count is less than or equal to 4 pages, it will be “Not Ok”
#If the document quality is good and the total page count is more than 4, it is “Ok”

#if document good but not ok (<5 pages) if conecept note passed then change to OK.
concept_note = ['background','contexte','fondo','rationale','raisonnement','concept','concepto','situation analysis','documento de proyecto','document de projet','analyse de la situation','análisis de situación', 'initiation plan','plan de iniciación',"plan d'initiation"]

#if document >5 pages but not good: if project_document passes then document passes to OK & good.
project_document = ['project document', 'documento de proyecto', 'document de projet']

awp= ['annual work plan', 'plan de trabajo','anual plan','de travail annuel','combined delivery report', 'rapport de livraison combiné','informe de entrega combinado','cdr','awp']

#if bad  & >10 pages then review
# if bad  & >5 pages and (concept note or project note TRUE) then OK
#if bad & 5<pages<=10 then review
#if bad & 5<pages<=10 & awp = True  then bad

i=0
for index,docrow in dfcsv[dfcsv['eval1'].isnull() & dfcsv['eval2'].isnull()].head(400).iterrows():
    i=i+1
    context = ssl._create_unverified_context()
    #print(docrow['url'])
    #download file - to do so need to replace spaces with defined spaces
    url = docrow['url']
    url = urllib.parse.urlsplit(url)
    url = list(url)
    url[2] = urllib.parse.quote(url[2])
    url = urllib.parse.urlunsplit(url)
    downloadfile = urllib.request.urlopen(url,context=context)
    
    page_content=""
    passer2 =""
    content_missing = ""
    
    #reading file, depending on filetype
    if docrow['doctype'] =='pdf':
        file = open("temp-file.pdf", 'wb')
        file.write(downloadfile.read())
        try:
            pdfReader = PdfFileReader("temp-file.pdf")
            passer = True
        except:
            print("A: File Corrupt: ", url)
            eval2 ='corrupt file'
            eval1 = 'bad'
            passer = False
        file.close()
        
        if passer == True:
            
            try:
                #counting pages
                pages = pdfReader.numPages

                #get content from first 5 pages
                for page_number in range(0,min(5,pages)):
                    page = pdfReader.getPage(page_number)
                    page_content += page.extractText().lower()
            except:
                try:
                    #sometimes it works the second time around
                    pages = pdfReader.numPages
                except:
                    try:
                        with pdfplumber.open("temp-file.pdf") as pdf:
                            pages = len(pdf.pages)
                    except:
                        pdf = pikepdf.Pdf.open('temp-file.pdf')
                        pages = len(pdf.pages)
                    
            try: #check if it is not ok or review, if that's the case runn through pike or plumber.
                project_title_cond  = bool([ele for ele in project_title if(ele in page_content)])
                summary_desc_cond  = bool([ele for ele in summary_desc if(ele in page_content)])
                project_id_cond  = bool([ele for ele in project_id if(ele in page_content)])
                implementing_partner_cond  = bool([ele for ele in implementing_partner if(ele in page_content)])
                start_date_cond  = bool([ele for ele in start_date if(ele in page_content)])
                end_date_cond  = bool([ele for ele in end_date if(ele in page_content)])
                budget_cond  = bool([ele for ele in budget if(ele in page_content)])
                project_document_cond = bool([ele for ele in project_document if(ele in page_content)])
                concept_note_cond = bool([ele for ele in concept_note if(ele in page_content)])
                awp_cond = bool([ele for ele in awp if(ele in page_content)])

                score = project_title_cond + summary_desc_cond +project_id_cond +implementing_partner_cond +start_date_cond +end_date_cond + budget_cond
            except:
                score = 0
            
            if pages == 0:
                try:
                    pdf = pikepdf.Pdf.open('temp-file.pdf')
                    pages = len(pdf.pages)
                except:
                    print("B: File corrupt", url)
                    passer2 = "fail"

            if ((len(page_content)<1000) & (passer2 !="fail")) | score<4:
                try:
                    file_out = open("temp-file-5p.pdf", 'wb')
                    file_in = [open("temp-file.pdf", "rb")]
                    pdfpages.extract(file_in, file_out, list(range(1,min(pages+1,6))))
                    file_out.close()
                    filoeoutname = 'temp-file-5p.pdf'
                except:
                    filoeoutname = 'temp-file.pdf'
                
                try:
                    with pdfplumber.open(filoeoutname) as pdf:
                        for pdf_page in pdf.pages:
                            page_content = page_content + pdf_page.extract_text()
                    page_content = str(page_content.lower())

                    os.remove("temp-file-5p.pdf")
                except:
                    try:
                        os.remove("temp-file-5p.pdf")
                        file_out = open("temp-file-5p.pdf", 'wb')
                        file_in = [open("temp-file.pdf", "rb")]
                        pdfpages.extract(file_in, file_out, list(range(1,2)))
                        file_out.close()
                        filoeoutname = 'temp-file-5p.pdf'
                        
                        
                        
                        with pdfplumber.open(filoeoutname) as pdf:
                            for pdf_page in pdf.pages:
                                page_content = page_content + pdf_page.extract_text()
                        page_content = str(page_content.lower())
                        
                        os.remove("temp-file-5p.pdf")
                    except:
                        try:
                            page_content = ""
                            with pdfplumber.open("temp-file.pdf") as pdf:
                                for pdf_page in pdf.pages:
                                    page_content = page_content + pdf_page.extract_text()
                            page_content = str(page_content.lower())
                        except:
                            try: 
                                page_content = textract.process(
                                    "temp-file.pdf",
                                    method='tesseract',
                                    language='eng',
                                )
                                page_content = str(page_content.lower())
                            except:
                                page_content =""
                                content_missing = "review"
                                print("Unable to read file.")
                        
                        
            
        os.remove("temp-file.pdf")
        
    elif docrow['doctype'] == 'docx':
        file = open("temp-file.docx", 'wb')
        file.write(downloadfile.read())

        text = textract.process("temp-file.docx")
        file.close()
        os.remove("temp-file.docx")
        
        #estimate number of pages based on character count. This is not accurate, but approximate
        pages = round(len(text)/2000)
        
        #get content from first 5 pages (approx)
        page_content = str(text[0:12000]).lower()
        
    elif docrow['doctype'] == 'doc':
        try:
            try:
                file = open("temp-file.doc", 'wb')
                file.write(downloadfile.read())

                text = textract.process("temp-file.doc")
                file.close()
                os.remove("temp-file.doc")

                #estimate number of pages based on character count. This is not accurate, but approximate
                pages = round(len(text)/2000)

                #get content from first 5 pages (approx)
                page_content = str(text[0:12000]).lower()
            except:
                try:
                    file.close()
                except:
                    pass
                try:
                    os.remove("temp-file.doc")
                except:
                    pass
                file = open("temp-file.docx", 'wb')
                file.write(downloadfile.read())

                text = textract.process("temp-file.docx")
                file.close()
                os.remove("temp-file.docx")

                #estimate number of pages based on character count. This is not accurate, but approximate
                pages = round(len(text)/2000)

                #get content from first 5 pages (approx)
                page_content = str(text[0:12000]).lower()
        except:
            print("C: File corrupt: ", url)
        
    else:
        page_content = ""
        pages = 0
        
    #Scoring mechanism based on keyword searches    
    project_title_cond  = bool([ele for ele in project_title if(ele in page_content)])
    summary_desc_cond  = bool([ele for ele in summary_desc if(ele in page_content)])
    project_id_cond  = bool([ele for ele in project_id if(ele in page_content)])
    implementing_partner_cond  = bool([ele for ele in implementing_partner if(ele in page_content)])
    start_date_cond  = bool([ele for ele in start_date if(ele in page_content)])
    end_date_cond  = bool([ele for ele in end_date if(ele in page_content)])
    budget_cond  = bool([ele for ele in budget if(ele in page_content)])
    project_document_cond = bool([ele for ele in project_document if(ele in page_content)])
    concept_note_cond = bool([ele for ele in concept_note if(ele in page_content)])
    awp_cond = bool([ele for ele in awp if(ele in page_content)])

    score = project_title_cond + summary_desc_cond +project_id_cond +implementing_partner_cond +start_date_cond +end_date_cond + budget_cond

    if score>=4:
        eval1 = 'good'
    else:
        eval1 = 'bad'

    if (pages>4) & (eval1 == 'good'):
        eval2 = 'ok'
    else:
        eval2 = 'not ok'

    if (eval1 == 'good') & (eval2 == 'not ok'):
        if concept_note_cond == True:
            eval2 = 'ok'

    if (eval1 == 'bad') & (eval2 == 'ok'):
        if project_document_cond == True:
            eval1 = 'good'
            
    if (eval1 == 'bad')  & (pages >10 ):
        eval2 = 'review'
        
    if (eval1 == 'bad')  & (pages >5 ) & ((concept_note_cond ==True ) | (project_document_cond ==True )):
        eval2 = 'ok'
        
    if (eval1 == 'bad')  & (pages >2 ) & (concept_note_cond ==True ): #new
        eval2 = 'ok'

    if (eval1 == 'bad') & (pages >5 ) & (pages <11 ) & (awp != True) :
        eval2 = 'review'
        
    if (eval1 == 'bad') & (pages >5 ) & (pages <11 ) & (awp == True) :
        eval1 = 'bad'
    
    if (content_missing == "review") & (eval2 == "not ok") :
        eval2 = "review"

    
    dfcsv.loc[index,'eval1'] = eval1
    dfcsv.loc[index,'eval2'] = eval2
    dfcsv.loc[index,'score'] = score
    dfcsv.loc[index,'pages'] = pages
    dfcsv.loc[index,'project_title_cond'] = project_title_cond
    dfcsv.loc[index,'summary_desc_cond'] = summary_desc_cond
    dfcsv.loc[index,'project_id_cond'] = project_id_cond
    dfcsv.loc[index,'implementing_partner_cond'] = implementing_partner_cond
    dfcsv.loc[index,'start_date_cond'] = start_date_cond
    dfcsv.loc[index,'end_date_cond'] = end_date_cond
    dfcsv.loc[index,'budget_cond'] = budget_cond
    dfcsv.loc[index,'concept_note_cond'] = concept_note_cond
    dfcsv.loc[index,'project_document_cond'] = project_document_cond
    dfcsv.loc[index,'awp_cond'] = awp_cond
    
    #print(eval1,eval2)
    print(str(i) + ": " + eval2)
    

dfcsv['project_id'] = dfcsv['project_id'].apply(lambda x: x.zfill(8))
dfcsv.to_csv('documents.csv',index=False)