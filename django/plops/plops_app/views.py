# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.shortcuts import render
from django.http import JsonResponse
from django.views.generic.base import View

import services
import json

def index(request):
    return render(request, 'plops_app/index.html')
      
class APIView(View):

    def get(self, request, *args, **kwargs):
        lat, lng = request.GET['lat'], request.GET['lng']
        results = services.fetchRealTimeData(lat, lng)
        return JsonResponse({'slots':results})

class HourlyAPIView(View):

    def get(self, request, *args, **kwargs):
        id = request.GET['id']
        results = services.fetchHourlyData(id)
        return JsonResponse({'hourly':results})
