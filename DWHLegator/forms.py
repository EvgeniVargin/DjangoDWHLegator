from django import forms
from .models import Employee

class PostForm(forms.ModelForm):
    class Meta:
        model = Employee
        fields = ('key','name','age','position','salary','bonus',)
