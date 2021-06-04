from django import forms
from .models import Group,Entity,SQLlist,Sign,Analitic,Label

class LabelForm(forms.ModelForm):

    class Meta:
        model = Label
        fields = ('caption','parent','ord','soft_type','view_name','deleted')

class SQLlistForm(forms.ModelForm):

    class Meta:
        model = SQLlist
        fields = ('sql_name','checkout_sql','checkout_sql_all','checkin_sql','deleted')

class GroupForm(forms.ModelForm):

    class Meta:
        model = Group
        fields = ('group_name','parent','strg_period','strg_period_type','deleted',)


class EntityForm(forms.ModelForm):

    class Meta:
        model = Entity
        fields = ('entity_name','parent','fct_table_name','hist_table_name','tmp_table_name','anlt_flg','deleted',)

class SignForm(forms.ModelForm):
    class Meta:
        model = Sign
        fields = ('sign_name','sign_descr','archive_flg','data_type','hist_flg','entity_id','sign_sql','mass_sql','condition','deleted')

class AnaliticForm(forms.ModelForm):
    class Meta:
        model = Analitic
        fields = ('effective_start','effective_end','anlt_code','anlt_name','archive_flg','entity_id','anlt_sql','anlt_alias','data_type','anlt_alias_descr','spec_import_sql','deleted')
