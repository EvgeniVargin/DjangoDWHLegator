# Generated by Django 3.1.2 on 2021-01-07 10:43

from django.db import migrations, models
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
        ('DWHLegator', '0021_auto_20210107_1213'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='group',
            name='model_name',
        ),
        migrations.AlterField(
            model_name='group',
            name='modified',
            field=models.DateTimeField(default=django.utils.timezone.now, null=True),
        ),
        migrations.AlterField(
            model_name='group',
            name='modified_by',
            field=models.CharField(default='auth.User', max_length=256, null=True, verbose_name='Изменена пользователем:'),
        ),
    ]
