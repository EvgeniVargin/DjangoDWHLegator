# Generated by Django 3.1.2 on 2020-11-08 10:09

from django.db import migrations, models
import django.db.models.deletion
import mptt.fields


class Migration(migrations.Migration):

    dependencies = [
        ('DWHLegator', '0006_auto_20201107_1854'),
    ]

    operations = [
        migrations.AlterField(
            model_name='label',
            name='soft_type',
            field=models.CharField(choices=[('DWH', 'DWH'), ('System', 'System')], default='DWH', max_length=256),
        ),
        migrations.CreateModel(
            name='Entity',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('entity_name', models.CharField(max_length=256, unique=True)),
                ('fct_table_name', models.CharField(max_length=256)),
                ('hist_table_name', models.CharField(max_length=256)),
                ('tmp_table_name', models.CharField(max_length=256)),
                ('anlt_flg', models.IntegerField(default=0)),
                ('lft', models.PositiveIntegerField(editable=False)),
                ('rght', models.PositiveIntegerField(editable=False)),
                ('tree_id', models.PositiveIntegerField(db_index=True, editable=False)),
                ('level', models.PositiveIntegerField(editable=False)),
                ('parent', mptt.fields.TreeForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, related_name='children', to='DWHLegator.entity')),
            ],
            options={
                'abstract': False,
            },
        ),
    ]