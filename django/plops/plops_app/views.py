# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.shortcuts import render
from django.http import HttpResponse
import postgres

def index(request):
    result = postgres.execute("select * from hist_occupancy limit 1")
    context = {'latest_question_list': str(result)}
    return render(request, 'plops_app/index.html', context)
    #return HttpResponse("Hello, world. You're at the polls index.")