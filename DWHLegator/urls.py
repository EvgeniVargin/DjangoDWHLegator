"""DWHLegatorSite URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/3.1/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.urls import path
from . import views,signs,entityes,groups,sqls,analitics

urlpatterns = [
    #Homepage
    path('', views.homepage, name='homepage'),
    #Login
    path('login/', views.LoginView.as_view(), name='login'),
    path('logout/', views.LogoutView.as_view(), name='logout'),
    #Label
    path('label/', views.label_list, name='label_list'),
    path('label/<int:pk>/', views.label_detail, name='label_detail'),
    path('label/checkin/<int:pk>/', views.label_checkin, name='label_checkin'),
    path('label/checkout/<int:pk>/', views.label_checkout, name='label_checkout'),
    path('label/add/', views.label_add, name='label_add'),
    path('label/import/', views.label_import, name='label_import'),
    path('label/truncate/', views.label_truncate, name='label_truncate'),
    path('label/<int:pk>/edit/', views.label_edit, name='label_edit'),
    path('label/<int:pk>/del/', views.label_del, name='label_del'),
    #Group
    path('group/', groups.group_list, name='group_list'),
    path('group/<int:pk>/', groups.group_detail, name='group_detail'),
    path('group/checkin/<int:pk>/', groups.group_checkin, name='group_checkin'),
    path('group/checkout/<int:pk>/', groups.group_checkout, name='group_checkout'),
    path('group/add/', groups.group_add, name='group_add'),
    path('group/import/', groups.group_import, name='group_import'),
    path('group/truncate/', groups.group_truncate, name='group_truncate'),
    path('group/<int:pk>/edit/', groups.group_edit, name='group_edit'),
    path('group/<int:pk>/del/', groups.group_del, name='group_del'),
    #Entity
    path('entity/', entityes.entity_list, name='entity_list'),
    path('entity/<int:pk>/', entityes.entity_detail, name='entity_detail'),
    path('entity/checkin/<int:pk>/', entityes.entity_checkin, name='entity_checkin'),
    path('entity/checkout/<int:pk>/', entityes.entity_checkout, name='entity_checkout'),
    path('entity/add/', entityes.entity_add, name='entity_add'),
    path('entity/import/', entityes.entity_import, name='entity_import'),
    path('entity/truncate/', entityes.entity_truncate, name='entity_truncate'),
    path('entity/<int:pk>/edit/', entityes.entity_edit, name='entity_edit'),
    path('entity/<int:pk>/del/', entityes.entity_del, name='entity_del'),
    #SQLQuery
    path('sqlquery/', sqls.sql_list, name='sql_list'),
    path('sqlquery/<int:pk>/', sqls.sql_detail, name='sql_detail'),
    path('sqlquery/checkin/<int:pk>/', sqls.sql_checkin, name='sql_checkin'),
    path('sqlquery/checkout/<int:pk>/', sqls.sql_checkout, name='sql_checkout'),
    path('sqlquery/add/', sqls.sql_add, name='sql_add'),
    path('sqlquery/<int:pk>/edit/', sqls.sql_edit, name='sql_edit'),
    path('sqlquery/<int:pk>/del/', sqls.sql_del, name='sql_del'),
    #Signs
    path('sign/', signs.sign_list, name='sign_list'),
    path('sign/<int:pk>/', signs.sign_detail, name='sign_detail'),
    path('sign/checkin/<int:pk>/', signs.sign_checkin, name='sign_checkin'),
    path('sign/checkout/<int:pk>/', signs.sign_checkout, name='sign_checkout'),
    path('sign/add/', signs.sign_add, name='sign_add'),
    path('sign/import/', signs.sign_import, name='sign_import'),
    path('sign/truncate/', signs.sign_truncate, name='sign_truncate'),
    path('sign/<int:pk>/edit/', signs.sign_edit, name='sign_edit'),
    path('sign/<int:pk>/del/', signs.sign_del, name='sign_del'),
    #Analitics
    path('analitic/', analitics.analitic_list, name='analitic_list'),
    path('analitic/<int:pk>/', analitics.analitic_detail, name='analitic_detail'),
    path('analitic/checkin/<int:pk>/', analitics.analitic_checkin, name='analitic_checkin'),
    path('analitic/checkout/<int:pk>/', analitics.analitic_checkout, name='analitic_checkout'),
    path('analitic/add/', analitics.analitic_add, name='analitic_add'),
    path('analitic/import/', analitics.analitic_import, name='analitic_import'),
    path('analitic/truncate/', analitics.analitic_truncate, name='analitic_truncate'),
    path('analitic/<int:pk>/edit/', analitics.analitic_edit, name='analitic_edit'),
    path('analitic/<int:pk>/del/', analitics.analitic_del, name='analitic_del'),

]
