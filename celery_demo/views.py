import json

from django.shortcuts import render

# Create your views here.
from django.http import HttpResponse

import time
from django.http import HttpResponseRedirect, HttpResponse
import os
# Create your views here.
from django.http import HttpResponse
from django.shortcuts import render
from django.views.decorators import csrf
from django.views.decorators.csrf import csrf_exempt
from get_db import get_db, add_task, job_finish
from log_ging.log_01 import *
import redis

redis_cli = redis.StrictRedis(host='localhost', port=6379, decode_responses=True)


# 接收POST请求数据
@csrf_exempt
def tasks(request):
    print('before run_test_suit')
    logger = create_logger()
    logger.info("开始接受请求")
    if request.POST:
        post_data = dict(request.POST)
        logger.info("接受请求成功")
        logger.info(post_data)
        try:
            if post_data['Type'][0] == "TAXDATA":
                if post_data['BatchID'] and post_data['BatchYear'] and post_data['BatchMonth'] and post_data[
                    'CompanyID'] and post_data['CustomerID'] and post_data['TaxId'] and post_data['TaxPwd'] and \
                        post_data[
                            'jobname'] and post_data['jobparams']:
                    account = post_data['TaxId'][0]
                    pwd = post_data['TaxPwd'][0]
                    batchid = post_data['BatchID'][0]
                    batchyear = int(post_data['BatchYear'][0])
                    batchmonth = int(post_data['BatchMonth'][0])
                    companyid = int(post_data['CompanyID'][0])
                    customerid = int(post_data['CustomerID'][0])
                    jobname = post_data['jobname'][0]
                    jobparams = post_data['jobparams'][0]
                    # 获取数据库
                    host, port, db = get_db(companyid)
                    # 添加任务
                    logger.info("添加任务到数据库")
                    logger.info(db)
                    add_task(host, port, db, batchid, batchyear, batchmonth, companyid, customerid, "TAXDATA", jobname,
                             jobparams)
                    logger.info("任务添加成功,开始爬取")
                    pdict = {"1": account, "2": pwd, "3": batchid, "4": batchyear, "5": batchmonth, "6": companyid,
                             "7": customerid, "8": host, "9": port, "10": db}
                    pjson = json.dumps(pdict)
                    redis_cli.lpush("szgslist", pjson)
                    # ss=redis_cli.lpop("list")
                    # print(redis_cli.lpop("list"))
                    # result=run_test_suit.delay(user=account, pwd=pwd, batchid=batchid, batchyear=batchyear, batchmonth=batchmonth,companyid=companyid, customerid=customerid,host=host,port=port,db=db)
                    return HttpResponse("job is runing background~")
                else:
                    return HttpResponse("wrong message")

            if post_data['Type'][0] == "CUSTOMERINFO":
                if post_data['BatchID'] and post_data[
                    'CompanyID'] and post_data['CustomerID'] and post_data[
                    'CustomerName'] and post_data['SocialID'] and post_data[
                    'jobname'] and post_data['jobparams']:
                    cn = post_data['CustomerName'][0]
                    sID = post_data['SocialID'][0]
                    batchid = post_data['BatchID'][0]
                    companyid = int(post_data['CompanyID'][0])
                    customerid = int(post_data['CustomerID'][0])
                    batchyear = int(post_data['BatchYear'][0])
                    batchmonth = int(post_data['BatchMonth'][0])
                    jobname = post_data['jobname'][0]
                    jobparams = post_data['jobparams'][0]

                    # 获取数据库
                    host, port, db = get_db(companyid)
                    # 添加任务
                    logger.info("添加任务到数据库")
                    logger.info(db)
                    add_task(host, port, db, batchid, batchyear, batchmonth, companyid, customerid, "CUSTOMERINFO",
                             jobname, jobparams)
                    logger.info("任务添加成功,开始爬取")
                    sz_credit_dict = {"1": cn, "2": sID, "3": batchid, "4": companyid,
                                      "5": customerid, "6": host, "7": port, "8": db}
                    pjson = json.dumps(sz_credit_dict)
                    redis_cli.lpush("sz_credit_list", pjson)
                    # ss=redis_cli.lpop("list")
                    # print(redis_cli.lpop("list"))
                    # result=run_test_suit.delay(user=account, pwd=pwd, batchid=batchid, batchyear=batchyear, batchmonth=batchmonth,companyid=companyid, customerid=customerid,host=host,port=port,db=db)
                    return HttpResponse("job is runing background~")
                else:
                    return HttpResponse("wrong message")

            if post_data['Type'][0] == "CUSTOMERINVOICE":
                if post_data['BatchID'] and post_data[
                    'CompanyID'] and post_data['CustomerID'] and post_data[
                    'Pwd'] and post_data['User'] and post_data[
                    'jobname'] and post_data['jobparams']:
                    user = post_data['User'][0]
                    pwd = post_data['Pwd'][0]
                    batchid = post_data['BatchID'][0]
                    companyid = int(post_data['CompanyID'][0])
                    customerid = int(post_data['CustomerID'][0])
                    batchyear = int(post_data['BatchYear'][0])
                    batchmonth = int(post_data['BatchMonth'][0])
                    jobname = post_data['jobname'][0]
                    jobparams = post_data['jobparams'][0]

                    # 获取数据库
                    host, port, db = get_db(companyid)
                    # 添加任务
                    logger.info("添加任务到数据库")
                    logger.info(db)
                    add_task(host, port, db, batchid, batchyear, batchmonth, companyid, customerid, "CUSTOMERINVOICE",
                             jobname, jobparams)
                    logger.info("任务添加成功,开始爬取")
                    fphz_dict = {"1": user, "2": pwd, "3": batchid, "4": companyid,
                                      "5": customerid, "6": host, "7": port, "8": db}
                    pjson = json.dumps(fphz_dict)
                    redis_cli.lpush("fphz_list", pjson)
                    return HttpResponse("job is runing background~")
                else:
                    return HttpResponse("wrong message")
        except:
            print("wrong message")


    return HttpResponse("input your message")
