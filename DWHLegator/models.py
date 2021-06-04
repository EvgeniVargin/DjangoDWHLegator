from django.conf import settings
from django.db import models
from django.utils import timezone
from mptt.models import MPTTModel, TreeForeignKey

# Create your models here.
class ModelSysFields(models.Model):
    created = models.DateTimeField(null=True,verbose_name = 'Дата и время создания:')
    created_by = models.CharField(max_length = 256,null=True,verbose_name = 'Создано пользователем:')
    modified = models.DateTimeField(default=timezone.now,null=True,verbose_name = 'Дата и время изменения:')
    modified_by = models.CharField(max_length = 256,null=True,verbose_name = 'Изменено пользователем:')
    checked_out = models.DateTimeField(null = True,verbose_name = 'Дата и время взятия на изменение:')
    checked_out_by = models.CharField(max_length = 256,null=True,verbose_name = 'Взято на изменение пользователем:')
    YES = 1
    NO = 0
    DELETED_IN_CHOICES = [(YES,'Да'),(NO,'Нет')]
    deleted = models.IntegerField(default = NO,choices=DELETED_IN_CHOICES,verbose_name='Удалено:')
    
    class Meta:
        abstract = True
    
    def _str_(self):
        return '(%s) %s'%(self.id,self.model_name)
    
    def add(self,name,user):
        self.created = timezone.now()
        self.created_by = user.username
        self.modified = timezone.now()
        self.modified_by = user.username
        self.checked_out = timezone.now()
        self.checked_out_by = user.username
        self.save()
        print('(ID=%s) %s created by %s %s'%(self.pk,name,self.created_by,self.created))
    
    def edit(self,name,user):
        self.modified = timezone.now()
        self.modified_by = self.checked_out_by
        self.save()
        print('(ID=%s) %s edited by %s %s'%(self.pk,name,self.modified_by,self.modified))
    
    def remove(self,name):
        self.deleted = 1
        self.modified = timezone.now()
        self.modified_by = self.checked_out_by
        self.save()
        print('(ID=%s) %s deleted by %s %s'%(self.pk,name,self.modified_by,self.modified))

    def checkout(self,name,user):
        self.checked_out = timezone.now()
        self.checked_out_by = user.username
        self.save()
        print('(ID=%s) %s checked out by %s %s'%(self.pk,name,self.checked_out_by,self.checked_out))

    def checkin(self,name):
        print('(ID=%s) %s checked in by %s %s'%(self.pk,name,self.checked_out_by,timezone.now()))
        self.checked_out = None
        self.checked_out_by = None
        self.save()
        
    def get_deleted_label(self):
        return self._meta.get_field('deleted').verbose_name.title()

    def get_created_label(self):
        return self._meta.get_field('created').verbose_name.title()

    def get_created_by_label(self):
        return self._meta.get_field('created_by').verbose_name.title()
    
    def get_modified_label(self):
        return self._meta.get_field('modified').verbose_name.title()

    def get_modified_by_label(self):
        return self._meta.get_field('modified_by').verbose_name.title()

    def get_checked_out_label(self):
        return self._meta.get_field('checked_out').verbose_name.title()

    def get_checked_out_by_label(self):
        return self._meta.get_field('checked_out_by').verbose_name.title()


class SQLlist(ModelSysFields):
    sql_name = models.CharField(max_length = 256,unique = True,verbose_name = 'Наименование:')
    checkout_sql = models.TextField(null = True,blank = True,verbose_name = 'CHECKOUT - запрос:')
    checkout_sql_all = models.TextField(null = True,blank = True,verbose_name = 'IMPORT - запрос:')
    checkin_sql = models.TextField(null = True,blank = True,verbose_name = 'CHECKIN - запрос:')

    def get_checkout_sql(self):
        return self.checkout_sql

    def get_checkout_sql_all(self):
        return self.checkout_sql_all

    def get_checkin_sql(self):
        return self.checkin_sql
    
    def __str__(self):
        return 'ID = %s; SQL_NAME = %s'%(self.id,self.sql_name)
        
class Entity(MPTTModel,ModelSysFields):
    entity_name = models.CharField(max_length = 256,unique=True,verbose_name='Наименование:')
    parent = TreeForeignKey('self', on_delete=models.CASCADE, null=True, blank=True,related_name='children',verbose_name='Родитель:')
    fct_table_name = models.CharField(max_length = 256,verbose_name='Таблица хранения по датам:')
    hist_table_name = models.CharField(max_length = 256,verbose_name='Таблица хранения периодами:')
    tmp_table_name = models.CharField(max_length = 256,verbose_name='Временная таблица:')
    YES = 1
    NO = 0
    ANLT_FLG_IN_CHOICES = [(YES,'Да'),(NO,'Нет')]
    anlt_flg = models.IntegerField(default = NO,verbose_name='Аналитика:',choices=ANLT_FLG_IN_CHOICES)
    
    class MPTTMeta:
        order_insertion_by = ['tree_id','level']

    def __str__(self):
        if self.parent is None:
            return '%s'%(self.entity_name)
        else:
            return '%s -> %s'%(self.parent,self.entity_name)
    
class Group(MPTTModel,ModelSysFields):
    group_name = models.CharField(max_length = 4000,unique=True)
    parent = TreeForeignKey('self', on_delete=models.CASCADE, null=True, blank=True, related_name='children')
    strg_period = models.IntegerField(null=True,blank=True)
    M = 'M'
    D = 'D'
    STRG_PERIOD_TYPE_CHOICES = [(M,'Месяцев'),(D,'Дней')]
    strg_period_type = models.CharField(max_length = 1,choices=STRG_PERIOD_TYPE_CHOICES,null=True,blank=True)
    
    class MPTTMeta:
        order_insertion_by = ['tree_id','level']

    def __str__(self):
        if self.parent is None:
            return '%s'%(self.group_name)
        else:
            return '%s -> %s'%(self.parent,self.group_name)

class Label(MPTTModel,ModelSysFields):
    caption = models.CharField(max_length = 256,unique=True,verbose_name = 'Наименование:')
    parent = TreeForeignKey('self', on_delete=models.CASCADE,null=True,blank=True,related_name='children',verbose_name = 'Родитель:')
    ord = models.IntegerField(verbose_name = 'Номер по порядку:')
    DWH = 'DWH'
    SYSTEM = 'System'
    SOFT_TYPE_IN_CHOICES = [(DWH,'DWH'),(SYSTEM,'System')]
    soft_type = models.CharField(max_length = 256,choices=SOFT_TYPE_IN_CHOICES,default=DWH,)
    view_name = models.CharField(max_length = 256,null=True,blank=True,verbose_name = 'URL:')
    
    class MPTTMeta:
        order_insertion_by = ['tree_id','level','ord']
        
    def __str__(self):
        if self.parent is None:
            return '%s'%(self.caption)
        else:
            return '%s -> %s'%(self.parent,self.caption)

class Sign(ModelSysFields):
    sign_name = models.CharField(max_length = 256,verbose_name = 'Наименование показателя:')
    sign_descr = models.CharField(max_length = 256,verbose_name = 'Описание показателя:')
    YES = 1
    NO = 0
    ACHIVE_FLG_IN_CHOICES = [(YES,'Да'),(NO,'Нет')]
    archive_flg = models.SmallIntegerField(choices = ACHIVE_FLG_IN_CHOICES,default = YES,verbose_name = 'Архивный:')
    NUM = 'Число'
    DT = 'Дата'
    STR = 'Строка'
    DATA_TYPE_IN_CHOICES = [(NUM,'Число'),(DT,'Дата'),(STR,'Строка')]
    data_type = models.CharField(max_length = 30,choices = DATA_TYPE_IN_CHOICES,default=STR,verbose_name = 'Тип данных:')
    FCT = 0
    HIST = 1
    HIST_FLG_IN_CHOICES = [(FCT,'По датам'),(HIST,'Периодами')]
    hist_flg = models.SmallIntegerField(choices = HIST_FLG_IN_CHOICES,default=HIST,verbose_name = 'Способ хранения:')
    sign_sql = models.TextField(null = True,blank = True,verbose_name = 'SQL для расчета за дату:')
    mass_sql = models.TextField(null = True,blank = True,verbose_name = 'SQL для массового расчета:')
    entity_id = models.ForeignKey(Entity,on_delete=models.PROTECT,verbose_name = 'Сущность:')
    condition = models.TextField(null = True,blank = True,verbose_name = 'Доп. условия для запуска расчета:')
 
    class MPTTMeta:
        ordering = ['sign_name']
        
    def __str__(self):
        return 'ID = %s; SIGN_NAME = %s; SIGN_DESCR = %s'%(self.id,self.sign_name,self.sign_descr)

class Analitic(ModelSysFields):
    effective_start = models.DateTimeField(default = timezone.now,null=True,verbose_name = 'Дата начала:')
    effective_end = models.DateTimeField(default = '31.12.5999 23:59:59',null=True,verbose_name = 'Дата окончания:')
    anlt_code = models.CharField(max_length = 256,null = True,verbose_name = 'Код:')
    anlt_name = models.CharField(max_length = 4000,null = True,verbose_name = 'Наименование:')
    YES = 1
    NO = 0
    ACHIVE_FLG_IN_CHOICES = [(YES,'Да'),(NO,'Нет')]
    archive_flg = models.SmallIntegerField(choices = ACHIVE_FLG_IN_CHOICES,default = YES,verbose_name = 'Архивная:')
    entity_id = models.ForeignKey(Entity,on_delete=models.PROTECT,verbose_name = 'Сущность:')
    anlt_sql = models.TextField(null = True,blank = True,verbose_name = 'SQL для раскраски фактов:')
    anlt_alias = models.CharField(max_length = 30,null = True,verbose_name = 'Альяс:')
    NUM = 'Число'
    DT = 'Дата'
    STR = 'Строка'
    DATA_TYPE_IN_CHOICES = [(NUM,'Число'),(DT,'Дата'),(STR,'Строка')]
    data_type = models.CharField(max_length = 30,choices = DATA_TYPE_IN_CHOICES,default=STR,verbose_name = 'Тип данных:')
    anlt_alias_descr = models.CharField(max_length = 1000,null = True,verbose_name = 'Описание альяса:')
    spec_import_sql = models.TextField(null = True,blank = True,verbose_name = 'SQL для импорта спецификации:')
    
    class MPTTMeta:
        ordering = ['anlt_name']
        
    def __str__(self):
        return 'ID = %s; ANLT_CODE = %s; ANLT_NAME = %s (%s - %s)'%(self.id,self.anlt_code,self.anlt_name,self.effective_start,self.effective_end)
