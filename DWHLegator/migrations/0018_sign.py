# Generated by Django 3.1.2 on 2020-11-16 11:44

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('DWHLegator', '0017_sqllist_osuser'),
    ]

    operations = [
        migrations.CreateModel(
            name='Sign',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('sign_name', models.CharField(max_length=256)),
                ('sign_descr', models.CharField(max_length=256)),
                ('archive_flg', models.SmallIntegerField(default=1)),
                ('data_type', models.CharField(max_length=30)),
                ('hist_flg', models.SmallIntegerField(default=1)),
                ('sign_sql', models.TextField(blank=True, null=True)),
                ('mass_sql', models.TextField(blank=True, null=True)),
                ('condition', models.TextField(blank=True, null=True)),
                ('entity_id', models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, to='DWHLegator.entity')),
            ],
        ),
    ]