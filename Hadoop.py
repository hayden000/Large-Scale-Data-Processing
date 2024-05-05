from email.parser import Parser
import re
import time
from datetime import datetime, timezone, timedelta

def date_to_dt(date):
    def to_dt(tms):
        def tz():
            return timezone(timedelta(seconds=tms.tm_gmtoff))
        return datetime(tms.tm_year, tms.tm_mon, tms.tm_mday, 
                      tms.tm_hour, tms.tm_min, tms.tm_sec, 
                      tzinfo=tz())
    return to_dt(time.strptime(date[:-6], '%a, %d %b %Y %H:%M:%S %z'))

def extract_email_network(rdd):
    def parse_email(record):
        email_parser = Parser()
        email_message = email_parser.parsestr(record)

        from_email = email_message['From']
        
        recipients = ''
        if email_message['To']:
            recipients += email_message['To']
        if email_message['Cc']:
            recipients += ', ' + email_message['Cc']
        if email_message['Bcc']:
            recipients += ', ' + email_message['Bcc']

        recipients_2 = recipients.replace(' ', '')
        recipients_3 = recipients_2.replace('\n', '')
        recipients_4 = recipients_3.replace('\t', '')
        recipient_list = recipients_4.split(',')
        date = email_message['Date']
        timestamp = date_to_dt(date)

        triples = []
        
        email_regex=r'^([A-Za-z0-9!#$%&\'*+-/=?^_`{|}~\.]+)@([A-Za-z0-9]+[\.])*enron\.com$'

        for recipient in recipient_list:
            if from_email != recipient:
                if bool(re.match(email_regex, recipient)):
                    if bool(re.match(email_regex, from_email)):
                        triples.append((from_email, recipient, timestamp))

        return triples

    return rdd.flatMap(parse_email).distinct()

def convert_to_weighted_network(rdd, drange=None):

    if drange is not None:
        rdd = rdd.filter(lambda x: drange[0] <= x[2] < drange[1])
    
    group = rdd.map(lambda x: ((x[0], x[1]), x[2])).groupByKey()

    def count(i):
        for key, times in i:
            yield (key[0], key[1],sum(1 for _ in times))

    weight = group.mapPartitions(count)

    return weight

def get_out_degrees(rdd):
    emails_sent = rdd.map(lambda x: (x[0], x[2]))
    all_emails_sent = emails_sent.reduceByKey(lambda a,b: a+b)

    allmap = rdd.flatMap(lambda x: [x[0],x[1]]).distinct()
    all_emails_sent = allmap.map(lambda n: (n, 0)).union(all_emails_sent).reduceByKey(lambda a,b: a+b)

    return all_emails_sent.map(lambda x:(x[1], x[0])).sortBy(lambda x: (x[0], x[1]), ascending=False)
      
def get_in_degrees(rdd):
    emails_sent = rdd.map(lambda x: (x[1], x[2]))
    all_emails_sent = emails_sent.reduceByKey(lambda a,b: a+b)

    allmap = rdd.flatMap(lambda x: [x[0],x[1]]).distinct()
    all_emails_sent = allmap.map(lambda n: (n, 0)).union(all_emails_sent).reduceByKey(lambda a,b: a+b)

    return all_emails_sent.map(lambda x:(x[1], x[0])).sortBy(lambda x: (x[0], x[1]), ascending=False)

        
def get_out_degree_dist(rdd):
    rdd_outs = get_out_degrees(rdd)
    distribution = rdd_outs.map(lambda x:(x[0], 1)).reduceByKey(lambda a,b: a+b)
    return distribution.sortByKey(ascending=True)

def get_in_degree_dist(rdd):
    rdd_ins = get_in_degrees(rdd)
    distribution = rdd_ins.map(lambda x:(x[0], 1)).reduceByKey(lambda a,b: a+b)
    return distribution.sortByKey(ascending=True)