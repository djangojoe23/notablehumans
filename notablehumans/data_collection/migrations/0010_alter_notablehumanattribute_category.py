# Generated by Django 5.0.11 on 2025-02-02 10:47

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('data_collection', '0009_alter_notablehumanattribute_category'),
    ]

    operations = [
        migrations.AlterField(
            model_name='notablehumanattribute',
            name='category',
            field=models.CharField(choices=[('gender', 'Gender'), ('occupation', 'Occupation'), ('ethnic_group', 'Ethnic Group'), ('field_of_work', 'Field of Work'), ('member_of', 'Member Of'), ('manner_of_death', 'Manner of Death'), ('cause_of_death', 'Cause of Death'), ('handedness', 'Handedness'), ('convicted_of', 'Convicted Of'), ('award_received', 'Award Received'), ('native_language', 'Native Language'), ('political_ideology', 'Political Ideology'), ('honorific_prefix', 'Honorific Prefix'), ('religion_of_worldview', 'Religion or Worldview'), ('medical_condition', 'Medical Condition'), ('conflict', 'Conflict'), ('educated_at', 'Educated At'), ('academic_degree', 'Academic Degree'), ('social_classification', 'Social Classification')], max_length=50),
        ),
    ]
