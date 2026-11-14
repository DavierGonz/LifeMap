"""Grafico KM offline usando Bokeh ya instalado; lee resultados, no reprocesa Yelp."""
import json
from pathlib import Path
import sys


def main():
    from bokeh.io import output_file, save
    from bokeh.layouts import column
    from bokeh.models import ColumnDataSource, Div, HoverTool
    from bokeh.plotting import figure
    from bokeh.resources import INLINE
    source = Path(sys.argv[1])
    data = json.loads(source.read_text())
    figures = []
    for kind, title in [('category', 'Categorias principales'), ('zone', 'Zonas del estudio')]:
        chart = figure(title=title+' — Kaplan-Meier retrospectivo', height=340,
                       sizing_mode='stretch_width', y_range=(0, 1.03),
                       x_axis_label='Dias desde primera reseña (no desde apertura)', y_axis_label='S(t)')
        selected = [c for c in data['curves'] if c['method']=='observed_origin' and c['group_type']==kind
                    and (kind=='zone' or c['group_id'] in data['config']['comparison_categories'])]
        for color, curve in zip(['#1565c0', '#c05b15'], selected):
            points = curve['points']
            fields = ['time_days', 'survival', 'at_risk', 'events', 'censored']
            values = ColumnDataSource({k:[p[k] for p in points] for k in fields})
            chart.step('time_days', 'survival', source=values, mode='after', color=color,
                       legend_label=curve['group_id'], line_width=2)
        chart.add_tools(HoverTool(tooltips=[('Dias','@time_days{0.0}'),('S(t)','@survival{0.000}'),
                         ('En riesgo','@at_risk'),('Eventos','@events'),('Censuras','@censored')]))
        chart.legend.location='bottom_left'
        figures.append(chart)
    note = Div(text='<b>Vida observada, no vida desde fundacion.</b> Cerrados: ultima reseña como proxy. '
                    'Abiertos: censura administrativa al corte Yelp. Grupos ≥50 negocios. '
                    'Colas con menos de 20 en riesgo requieren cautela. Sin interpretacion causal ni uso predictivo.')
    output_file(source.with_name('comparison.html'), title='LifeMap — supervivencia observada')
    save(column(note, *figures, sizing_mode='stretch_width'), resources=INLINE)
    print('Saved offline comparison.html')


if __name__ == '__main__':
    main()
