from django.urls import path
from .views import *

urlpatterns = [
    path('test', test, name='test'),
    path('get_info', get_info, name='get_info'),
    path('get_info_detail', get_info_detail, name='get_info_detail')
]