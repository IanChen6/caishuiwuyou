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
from get_db import get_db, add_task, job_finish, cancelling_task
from log_ging.log_01 import *
import redis
import collections

redis_cli = redis.StrictRedis(host='localhost', port=6379, decode_responses=True)
logger = create_logger()


# 接收POST请求数据
@csrf_exempt
def tasks(request):
    print('before run_test_suit')
    logger.info("开始接受请求")
    if request.POST:
        post_data = dict(request.POST)
        logger.info("接受请求成功")
        logger.info(post_data)
        # logger.info(len(post_data))
        try:
            if post_data['Type'][0] == "TAXDATA":
                logger.info("爬取国税、地税任务")
                if post_data['BatchID'] and post_data['BatchYear'] and post_data['BatchMonth'] and post_data[
                    'CompanyID'] and post_data['CustomerID'] and post_data['TaxId'] and post_data['TaxPwd'] and \
                        post_data[
                            'jobname'] and post_data['jobparams']:
                    logger.info("任务信息接收成功")
                    account = post_data['TaxId'][0]
                    pwd = post_data['TaxPwd'][0]
                    batchid = post_data['BatchID'][0]
                    batchyear = int(post_data['BatchYear'][0])
                    batchmonth = int(post_data['BatchMonth'][0])
                    companyid = int(post_data['CompanyID'][0])
                    customerid = int(post_data['CustomerID'][0])
                    jobname = post_data['jobname'][0]
                    jobparams = post_data['jobparams'][0]
                    try:
                        jobextend = post_data['JobExtend'][0]
                    except:
                        jobextend=''
                        pass
                    # 获取数据库
                    host, port, db = get_db(companyid)
                    # 添加任务
                    logger.info("添加任务到数据库")
                    logger.info(db)
                    add_task(host, port, db, batchid, batchyear, batchmonth, companyid, customerid, "TAXDATA", jobname,
                             jobparams, jobextend)
                    print(jobextend)
                    logger.info("任务添加成功,开始爬取")
                    pdict = {"1": account, "2": pwd, "3": batchid, "4": batchyear, "5": batchmonth, "6": companyid,
                             "7": customerid, "8": host, "9": port, "10": db}
                    collections.OrderedDict()
                    pjson = json.dumps(pdict)
                    redis_cli.lpush("szgslist", pjson)
                    # ss=redis_cli.lpop("list")
                    # print(redis_cli.lpop("list"))
                    # result=run_test_suit.delay(user=account, pwd=pwd, batchid=batchid, batchyear=batchyear, batchmonth=batchmonth,companyid=companyid, customerid=customerid,host=host,port=port,db=db)
                    return HttpResponse("job is runing background~")
                else:
                    return HttpResponse("wrong message")

            if post_data['Type'][0] == "CUSTOMERINFO":
                logger.info("爬取信用信息")
                if post_data['BatchID'] and post_data[
                    'CompanyID'] and post_data['CustomerID'] and post_data[
                    'TaxId'] and post_data['TaxPwd'] and post_data[
                    'jobname'] and post_data['jobparams']:
                    logger.info("任务信息接收成功")
                    user = post_data['TaxId'][0]
                    pwd = post_data['TaxPwd'][0]
                    batchid = post_data['BatchID'][0]
                    companyid = int(post_data['CompanyID'][0])
                    customerid = int(post_data['CustomerID'][0])
                    batchyear = int(post_data['BatchYear'][0])
                    batchmonth = int(post_data['BatchMonth'][0])
                    jobname = post_data['jobname'][0]
                    jobparams = post_data['jobparams'][0]
                    try:
                        companyname=post_data['CustomerName'][0]
                    except:
                        companyname=""
                    try:
                        jobextend = post_data['JobExtend'][0]
                    except:
                        jobextend=''
                        pass
                    # 获取数据库
                    host, port, db = get_db(companyid)
                    # 添加任务
                    logger.info("添加任务到数据库")
                    logger.info(db)
                    add_task(host, port, db, batchid, batchyear, batchmonth, companyid, customerid, "CUSTOMERINFO",
                             jobname, jobparams, jobextend)
                    print(jobextend)
                    logger.info("任务添加成功,开始爬取")
                    sz_credit_dict = {"1": user, "2": pwd, "3": batchid, "4": companyid,
                                      "5": customerid, "6": host, "7": port, "8": db,"9":companyname}
                    pjson = json.dumps(sz_credit_dict)
                    redis_cli.lpush("sz_credit_list", pjson)
                    # ss=redis_cli.lpop("list")
                    # print(redis_cli.lpop("list"))
                    # result=run_test_suit.delay(user=account, pwd=pwd, batchid=batchid, batchyear=batchyear, batchmonth=batchmonth,companyid=companyid, customerid=customerid,host=host,port=port,db=db)
                    return HttpResponse("job is runing background~")
                else:
                    return HttpResponse("wrong message")

            if post_data['Type'][0] == "CUSTOMERINVOICE":
                logger.info("发票汇总任务")
                if post_data['BatchID'] and post_data[
                    'CompanyID'] and post_data['CustomerID'] and post_data[
                    'TaxPwd'] and post_data['TaxId'] and post_data[
                    'jobname'] and post_data['jobparams']:
                    logger.info("任务信息接收成功")
                    user = post_data['TaxId'][0]
                    pwd = post_data['TaxPwd'][0]
                    batchid = post_data['BatchID'][0]
                    companyid = int(post_data['CompanyID'][0])
                    customerid = int(post_data['CustomerID'][0])
                    batchyear = int(post_data['BatchYear'][0])
                    batchmonth = int(post_data['BatchMonth'][0])
                    jobname = post_data['jobname'][0]
                    jobparams = post_data['jobparams'][0]
                    try:
                        jobextend = post_data['JobExtend'][0]
                    except:
                        jobextend=''
                        pass
                    # 获取数据库
                    host, port, db = get_db(companyid)
                    # 添加任务
                    logger.info("添加任务到数据库")
                    logger.info(db)
                    add_task(host, port, db, batchid, batchyear, batchmonth, companyid, customerid, "CUSTOMERINVOICE",
                             jobname, jobparams, jobextend)
                    print(jobextend)
                    logger.info("任务添加成功,开始爬取")
                    fphz_dict = {"1": user, "2": pwd, "3": batchid, "4": companyid,
                                 "5": customerid, "6": host, "7": port, "8": db}
                    pjson = json.dumps(fphz_dict)
                    redis_cli.lpush("fphz_list", pjson)
                    return HttpResponse("job is runing background~")
                else:
                    return HttpResponse("wrong message")

            if post_data['Type'][0] == "TAXAPPLYNORMAL":
                logger.info("自动申报任务")
                if post_data['BatchID'] and post_data['BatchYear'] and post_data['BatchMonth'] and post_data[
                    'CompanyID'] and post_data['CustomerID'] and post_data['TaxId'] and post_data['TaxPwd'] and \
                        post_data['jobname'] and post_data['jobparams'] and post_data['fw1'] and post_data['fw2'] and \
                        post_data['hw1'] and post_data['hw2'] and post_data['fwms'] and post_data['fwyj'] and post_data[
                    'hwyj'] and post_data['hwms'] and post_data['djzs'] and post_data['dfzs'] and post_data['dczs'] and \
                        post_data['djms'] and post_data['dfms'] and post_data['dcms']:
                    logger.info("任务信息接收成功")
                    user = post_data['TaxId'][0]
                    pwd = post_data['TaxPwd'][0]
                    batchid = post_data['BatchID'][0]
                    batchyear = int(post_data['BatchYear'][0])
                    batchmonth = int(post_data['BatchMonth'][0])
                    companyid = int(post_data['CompanyID'][0])
                    customerid = int(post_data['CustomerID'][0])
                    jobname = post_data['jobname'][0]
                    jobparams = post_data['jobparams'][0]
                    fw1 = post_data['fw1'][0]
                    fw2 = post_data['fw2'][0]
                    hw1 = post_data['hw1'][0]
                    hw2 = post_data['hw2'][0]
                    fwms = post_data['fwms'][0]
                    fwyj = post_data['fwyj'][0]
                    hwyj = post_data['hwyj'][0]
                    hwms = post_data['hwms'][0]
                    djzs = post_data['djzs'][0]
                    dfzs = post_data['dfzs'][0]
                    dczs = post_data['dczs'][0]
                    djms = post_data['djms'][0]
                    dfms = post_data['dfms'][0]
                    dcms = post_data['dcms'][0]
                    try:
                        jobextend = post_data['JobExtend'][0]
                    except:
                        jobextend=''
                        pass
                    # 获取数据库
                    host, port, db = get_db(companyid)
                    # 添加任务
                    logger.info("添加任务到数据库")
                    logger.info(db)
                    add_task(host, port, db, batchid, batchyear, batchmonth, companyid, customerid, "TAXAPPLYNORMAL",
                             jobname, jobparams, jobextend)
                    print(jobextend)
                    logger.info("任务添加成功,开始爬取")
                    pdict = {"1": user, "2": pwd, "3": batchid, "4": batchyear, "5": batchmonth, "6": companyid,
                             "7": customerid, "8": host, "9": port, "10": db, "11": fw1, "12": fw2, "13": hw1,
                             "14": hw2, "15": fwms, "16": fwyj, "17": hwyj, "18": hwms, "19": djzs, "20": dfzs,
                             "21": dczs, "22": djms, "23": dfms, "24": dcms}
                    pjson = json.dumps(pdict)
                    redis_cli.lpush("zdsblist", pjson)
                    # ss=redis_cli.lpop("list")
                    # print(redis_cli.lpop("list"))
                    # result=run_test_suit.delay(user=account, pwd=pwd, batchid=batchid, batchyear=batchyear, batchmonth=batchmonth,companyid=companyid, customerid=customerid,host=host,port=port,db=db)
                    return HttpResponse("job is runing background~")
                else:
                    return HttpResponse("wrong message")

            if post_data['Type'][0] == "TAXINVOICE":
                logger.info("抓代开发票")
                if post_data['BatchID'] and post_data['BatchYear'] and post_data['BatchMonth'] and post_data[
                    'CompanyID'] and post_data['CustomerID'] and post_data['TaxId'] and post_data['TaxPwd'] and \
                        post_data['jobname'] and post_data['jobparams']:
                    logger.info("任务信息接收成功")
                    user = post_data['TaxId'][0]
                    pwd = post_data['TaxPwd'][0]
                    batchid = post_data['BatchID'][0]
                    batchyear = int(post_data['BatchYear'][0])
                    batchmonth = int(post_data['BatchMonth'][0])
                    companyid = int(post_data['CompanyID'][0])
                    customerid = int(post_data['CustomerID'][0])
                    jobname = post_data['jobname'][0]
                    jobparams = post_data['jobparams'][0]
                    try:
                        jobextend = post_data['JobExtend'][0]
                    except:
                        jobextend=''
                        pass
                    # 获取数据库
                    host, port, db = get_db(companyid)
                    # 添加任务
                    logger.info("添加任务到数据库")
                    logger.info(db)
                    add_task(host, port, db, batchid, batchyear, batchmonth, companyid, customerid, "TAXINVOICE",
                             jobname, jobparams, jobextend)
                    print(jobextend)
                    logger.info("任务添加成功,开始爬取")
                    pdict = {"1": user, "2": pwd, "3": batchid, "4": companyid, "5": customerid, "6": host, "7": port,
                             "8": db}
                    pjson = json.dumps(pdict)
                    redis_cli.lpush("daikai", pjson)
                    # ss=redis_cli.lpop("list")
                    # print(redis_cli.lpop("list"))
                    # result=run_test_suit.delay(user=account, pwd=pwd, batchid=batchid, batchyear=batchyear, batchmonth=batchmonth,companyid=companyid, customerid=customerid,host=host,port=port,db=db)
                    return HttpResponse("job is runing background~")
                else:
                    return HttpResponse("wrong message")

        except Exception as e:
            print("wrong message")
            logger.warn(e)

    return HttpResponse("input your message")


# 接收POST请求数据
@csrf_exempt
def cancel_task(request):
    print('before run_test_suit')
    logger.info("开始接受请求")
    if request.POST:
        logger.info('接受到post请求')
        post_data = dict(request.POST)
        logger.info("接受请求成功")
        logger.info(post_data)
        try:
            if post_data['Type'][0] == "TAXDATA":
                logger.info('取消国税、地税任务')
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
                    logger.info("取消任务到数据库")
                    logger.info(db)
                    cancelling_task(host, port, db, batchid, batchyear, batchmonth, companyid, customerid, "TAXDATA",
                                    jobname, jobparams)
                    logger.info("取消任务已添加到数据库")
                    # pdict = {"1": account, "2": pwd, "3": batchid, "4": batchyear, "5": batchmonth, "6": companyid,
                    #          "7": customerid, "8": host, "9": port, "10": db}
                    # pjson = json.dumps(pdict)
                    a = redis_cli.lrange('szgslist', 0, -1)
                    for i in a:
                        if batchid in i:
                            redis_cli.lrem('szgslist', 1, i)
                    # ss=redis_cli.lpop("list")
                    # print(redis_cli.lpop("list"))
                    # result=run_test_suit.delay(user=account, pwd=pwd, batchid=batchid, batchyear=batchyear, batchmonth=batchmonth,companyid=companyid, customerid=customerid,host=host,port=port,db=db)
                    return HttpResponse("job is cancelled successfully~")
                else:
                    return HttpResponse("wrong message")

            if post_data['Type'][0] == "CUSTOMERINFO":
                logger.info("爬取信用信息")
                if post_data['bathcancel']:
                    logger.info("任务信息接收成功")
                    batchid = post_data['bathcancel'][0]
                    #
                    logger.info("取消任务")
                    a = redis_cli.lrange('sz_credit_list', 0, -1)
                    for i in a:
                        for batch in batchid:
                            if batch in i:
                                redis_cli.lrem('sz_credit_list', 1, i)
                                logger.info("取消成功")
                                print("取消成功")
                    # ss=redis_cli.lpop("list")
                    # print(redis_cli.lpop("list"))
                    # result=run_test_suit.delay(user=account, pwd=pwd, batchid=batchid, batchyear=batchyear, batchmonth=batchmonth,companyid=companyid, customerid=customerid,host=host,port=port,db=db)
                    return HttpResponse("job is cancelled successfully~")
                if post_data['BatchID'] and post_data[
                    'CompanyID'] and post_data['CustomerID'] and post_data[
                    'TaxId'] and post_data['TaxPwd'] and post_data[
                    'jobname'] and post_data['jobparams']:
                    logger.info("任务信息接收成功")
                    user = post_data['TaxId'][0]
                    pwd = post_data['TaxPwd'][0]
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
                    logger.info("取消任务到数据库")
                    logger.info(db)
                    cancelling_task(host, port, db, batchid, batchyear, batchmonth, companyid, customerid,
                                    "CUSTOMERINFO", jobname, jobparams)
                    logger.info("取消任务已添加到数据库")
                    # sz_credit_dict = {"1": user, "2": pwd, "3": batchid, "4": companyid,
                    #                   "5": customerid, "6": host, "7": port, "8": db}
                    # pjson = json.dumps(sz_credit_dict)
                    a = redis_cli.lrange('sz_credit_list', 0, -1)
                    for i in a:
                        if batchid in i:
                            redis_cli.lrem('sz_credit_list', 1, i)
                    # ss=redis_cli.lpop("list")
                    # print(redis_cli.lpop("list"))
                    # result=run_test_suit.delay(user=account, pwd=pwd, batchid=batchid, batchyear=batchyear, batchmonth=batchmonth,companyid=companyid, customerid=customerid,host=host,port=port,db=db)
                    return HttpResponse("job is cancelled successfully~")
                else:
                    return HttpResponse("wrong message")

            if post_data['Type'][0] == "CUSTOMERINVOICE":
                logger.info('取消发票汇总任务')
                if post_data['BatchID'] and post_data[
                    'CompanyID'] and post_data['CustomerID'] and post_data[
                    'TaxPwd'] and post_data['TaxId'] and post_data[
                    'jobname'] and post_data['jobparams']:
                    user = post_data['TaxId'][0]
                    pwd = post_data['TaxPwd'][0]
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
                    logger.info("取消任务到数据库")
                    logger.info(db)
                    cancelling_task(host, port, db, batchid, batchyear, batchmonth, companyid, customerid,
                                    "CUSTOMERINVOICE", jobname, jobparams)
                    logger.info("取消任务已添加到数据库")
                    # fphz_dict = {"1": user, "2": pwd, "3": batchid, "4": companyid,
                    #              "5": customerid, "6": host, "7": port, "8": db}
                    # pjson = json.dumps(fphz_dict)
                    # redis_cli.lrem("fphz_list", 1, pjson)
                    a = redis_cli.lrange('fphz_list', 0, -1)
                    for i in a:
                        if batchid in i:
                            redis_cli.lrem('fphz_list', 1, i)
                    return HttpResponse("job is cancelled successfully~")
                else:
                    return HttpResponse("wrong message")

            if post_data['Type'][0] == "TAXAPPLYNORMAL":
                logger.info('取消自动申报任务')
                if post_data['BatchID'] and post_data['BatchYear'] and post_data['BatchMonth'] and post_data[
                    'CompanyID'] and post_data['CustomerID'] and post_data['TaxId'] and post_data['TaxPwd'] and \
                        post_data[
                            'jobname'] and post_data['jobparams'] and post_data['fw1'] and post_data['fw2'] and \
                        post_data['hw1'] and post_data['hw2'] and post_data['fwms'] and post_data['fwyj'] and post_data[
                    'hwyj'] and post_data['hwms'] and post_data['djzs'] and post_data['dfzs'] and post_data['dczs'] and \
                        post_data['djms'] and post_data['dfms'] and post_data['dcms']:
                    user = post_data['TaxId'][0]
                    pwd = post_data['TaxPwd'][0]
                    batchid = post_data['BatchID'][0]
                    batchyear = int(post_data['BatchYear'][0])
                    batchmonth = int(post_data['BatchMonth'][0])
                    companyid = int(post_data['CompanyID'][0])
                    customerid = int(post_data['CustomerID'][0])
                    jobname = post_data['jobname'][0]
                    jobparams = post_data['jobparams'][0]
                    fw1 = post_data['fw1'][0]
                    fw2 = post_data['fw2'][0]
                    hw1 = post_data['hw1'][0]
                    hw2 = post_data['hw2'][0]
                    fwms = post_data['fwms'][0]
                    fwyj = post_data['fwyj'][0]
                    hwyj = post_data['hwyj'][0]
                    hwms = post_data['hwms'][0]
                    djzs = post_data['djzs'][0]
                    dfzs = post_data['dfzs'][0]
                    dczs = post_data['dczs'][0]
                    djms = post_data['djms'][0]
                    dfms = post_data['dfms'][0]
                    dcms = post_data['dcms'][0]

                    # 获取数据库
                    host, port, db = get_db(companyid)
                    # 添加任务
                    logger.info("取消任务到数据库")
                    logger.info(db)
                    cancelling_task(host, port, db, batchid, batchyear, batchmonth, companyid, customerid,
                                    "TAXAPPLYNORMAL", jobname, jobparams)
                    logger.info("取消任务已添加到数据库")
                    # pdict = {"1": user, "2": pwd, "3": batchid, "4": batchyear, "5": batchmonth, "6": companyid,
                    #          "7": customerid, "8": host, "9": port, "10": db, "11": fw1, "12": fw2, "13": hw1,
                    #          "14": hw2, "15": fwms, "16": fwyj, "17": hwyj, "18": hwms, "19": djzs, "20": dfzs,
                    #          "21": dczs, "22": djms, "23": dfms, "24": dcms}
                    # pjson = json.dumps(pdict)
                    # redis_cli.lrem("zdsblist", 1, pjson)
                    a = redis_cli.lrange('zdsblist', 0, -1)
                    for i in a:
                        if batchid in i:
                            redis_cli.lrem('zdsblist', 1, i)
                    # ss=redis_cli.lpop("list")
                    # print(redis_cli.lpop("list"))
                    # result=run_test_suit.delay(user=account, pwd=pwd, batchid=batchid, batchyear=batchyear, batchmonth=batchmonth,companyid=companyid, customerid=customerid,host=host,port=port,db=db)
                    return HttpResponse("job is cancelled successfully~")
                else:
                    return HttpResponse("wrong message")

            if post_data['Type'][0] == "TAXINVOICE":
                logger.info("取消代开发票")
                if post_data['BatchID'] and post_data['BatchYear'] and post_data['BatchMonth'] and post_data[
                    'CompanyID'] and post_data['CustomerID'] and post_data['TaxId'] and post_data['TaxPwd'] and \
                        post_data['jobname'] and post_data['jobparams']:
                    logger.info("任务信息接收成功")
                    user = post_data['TaxId'][0]
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
                    logger.info("取消任务到数据库")
                    logger.info(db)
                    cancelling_task(host, port, db, batchid, batchyear, batchmonth, companyid, customerid,
                                    "TAXINVOICE", jobname, jobparams)
                    logger.info("开始取消任务")
                    a = redis_cli.lrange('daikai', 0, -1)
                    for i in a:
                        if batchid in i:
                            redis_cli.lrem('daikai', 1, i)
                    # pdict = {"1": user, "2": pwd, "3": batchid,  "4": companyid, "5": customerid, "6": host, "7": port, "8": db}
                    # pjson = json.dumps(pdict)
                    # redis_cli.lpush("daikai", pjson)
                    # ss=redis_cli.lpop("list")
                    # print(redis_cli.lpop("list"))
                    # result=run_test_suit.delay(user=account, pwd=pwd, batchid=batchid, batchyear=batchyear, batchmonth=batchmonth,companyid=companyid, customerid=customerid,host=host,port=port,db=db)
                    return HttpResponse("job is runing background~")
                else:
                    return HttpResponse("wrong message")

        except Exception as e:
            print(e)
            print("wrong message")

    return HttpResponse("input your message")
