# Generated by Django 3.1.2 on 2020-11-16 11:53

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ('DWHLegator', '0018_sign'),
    ]

    operations = [
        migrations.AddField(
            model_name='sign',
            name='checkout_user',
            field=models.CharField(blank=True, max_length=256, null=True),
        ),
        migrations.AddField(
            model_name='sign',
            name='deleted',
            field=models.SmallIntegerField(default=0),
        ),
        migrations.AddField(
            model_name='sign',
            name='osuser',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, to=settings.AUTH_USER_MODEL),
        ),
        migrations.AlterField(
            model_name='entity',
            name='osuser',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, to=settings.AUTH_USER_MODEL),
        ),
        migrations.AlterField(
            model_name='group',
            name='osuser',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, to=settings.AUTH_USER_MODEL),
        ),
        migrations.AlterField(
            model_name='label',
            name='osuser',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, to=settings.AUTH_USER_MODEL),
        ),
        migrations.AlterField(
            model_name='sign',
            name='data_type',
            field=models.CharField(choices=[('Число', 'Число'), ('Дата', 'Дата'), ('Строка', 'Строка')], default='Строка', max_length=30),
        ),
        migrations.AlterField(
            model_name='sign',
            name='hist_flg',
            field=models.SmallIntegerField(choices=[(0, 'По датам'), (1, 'Периодами')], default=1),
        ),
    ]