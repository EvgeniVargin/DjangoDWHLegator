# Generated by Django 3.1.2 on 2021-01-08 04:21

from django.db import migrations, models
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
        ('DWHLegator', '0027_auto_20210107_2032'),
    ]

    operations = [
        migrations.AlterField(
            model_name='entity',
            name='checked_out',
            field=models.DateTimeField(null=True, verbose_name='Дата и время взятия на изменение:'),
        ),
        migrations.AlterField(
            model_name='entity',
            name='checked_out_by',
            field=models.CharField(max_length=256, null=True, verbose_name='Взято на изменение пользователем:'),
        ),
        migrations.AlterField(
            model_name='entity',
            name='created',
            field=models.DateTimeField(null=True, verbose_name='Дата и время создания:'),
        ),
        migrations.AlterField(
            model_name='entity',
            name='created_by',
            field=models.CharField(max_length=256, null=True, verbose_name='Создано пользователем:'),
        ),
        migrations.AlterField(
            model_name='entity',
            name='modified',
            field=models.DateTimeField(default=django.utils.timezone.now, null=True, verbose_name='Дата и время изменения:'),
        ),
        migrations.AlterField(
            model_name='entity',
            name='modified_by',
            field=models.CharField(max_length=256, null=True, verbose_name='Изменено пользователем:'),
        ),
        migrations.AlterField(
            model_name='group',
            name='checked_out',
            field=models.DateTimeField(null=True, verbose_name='Дата и время взятия на изменение:'),
        ),
        migrations.AlterField(
            model_name='group',
            name='checked_out_by',
            field=models.CharField(max_length=256, null=True, verbose_name='Взято на изменение пользователем:'),
        ),
        migrations.AlterField(
            model_name='group',
            name='created',
            field=models.DateTimeField(null=True, verbose_name='Дата и время создания:'),
        ),
        migrations.AlterField(
            model_name='group',
            name='created_by',
            field=models.CharField(max_length=256, null=True, verbose_name='Создано пользователем:'),
        ),
        migrations.AlterField(
            model_name='group',
            name='modified',
            field=models.DateTimeField(default=django.utils.timezone.now, null=True, verbose_name='Дата и время изменения:'),
        ),
        migrations.AlterField(
            model_name='group',
            name='modified_by',
            field=models.CharField(max_length=256, null=True, verbose_name='Изменено пользователем:'),
        ),
        migrations.AlterField(
            model_name='sqllist',
            name='checked_out',
            field=models.DateTimeField(null=True, verbose_name='Дата и время взятия на изменение:'),
        ),
        migrations.AlterField(
            model_name='sqllist',
            name='checked_out_by',
            field=models.CharField(max_length=256, null=True, verbose_name='Взято на изменение пользователем:'),
        ),
        migrations.AlterField(
            model_name='sqllist',
            name='created',
            field=models.DateTimeField(null=True, verbose_name='Дата и время создания:'),
        ),
        migrations.AlterField(
            model_name='sqllist',
            name='created_by',
            field=models.CharField(max_length=256, null=True, verbose_name='Создано пользователем:'),
        ),
        migrations.AlterField(
            model_name='sqllist',
            name='modified',
            field=models.DateTimeField(default=django.utils.timezone.now, null=True, verbose_name='Дата и время изменения:'),
        ),
        migrations.AlterField(
            model_name='sqllist',
            name='modified_by',
            field=models.CharField(max_length=256, null=True, verbose_name='Изменено пользователем:'),
        ),
    ]