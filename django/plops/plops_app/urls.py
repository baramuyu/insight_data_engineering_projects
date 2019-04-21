from django.conf.urls import url

from . import views

urlpatterns = [
    url(r'^$', views.index, name='index'),
    url(r'^api/$', views.APIView.as_view(), name='api-view'),
    url(r'^hourly_api/$', views.HourlyAPIView.as_view(), name='hourly-api-view'),
]
