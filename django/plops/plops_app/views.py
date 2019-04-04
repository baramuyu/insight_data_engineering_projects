# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.shortcuts import render
from django.http import HttpResponse
import services
import json

def index(request):
    results = services.fetchData() #[(34214, Decimal('4.0'), 14, u'POINT (47.60580762 -122.33341762)')]

    context = {'results': results}
    return render(request, 'plops_app/index.html', context)
    #return HttpResponse("Hello, world. You're at the polls index.")
    
