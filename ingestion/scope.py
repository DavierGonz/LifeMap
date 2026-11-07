"""Impide que el analisis exploratorio se convierta en carga definitiva."""
def validate_selection(selection,analysis):
    if selection.get('status')!='approved_by_user':
        raise ValueError('Area/family selection is not approved by the user')
    if selection.get('dataset_version')!=analysis['dataset_version'] or selection.get('analysis_fingerprint')!=analysis['fingerprint']:
        raise ValueError('Selection does not match the verified dataset/analysis')
    choices=[c for c in analysis['combinations'] if c['area_id']==selection.get('area_id') and c['family']==selection.get('family')]
    if len(choices)!=1 or choices[0]['unique_clean_reviews']<1_000_000:
        raise ValueError('Selected scope has fewer than one million verified clean unique reviews')
    return choices[0]
