<?xml version="1.0" encoding="UTF-8"?>
<StyledLayerDescriptor xmlns="http://www.opengis.net/sld" version="1.1.0" xmlns:ogc="http://www.opengis.net/ogc" xmlns:se="http://www.opengis.net/se" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.opengis.net/sld http://schemas.opengis.net/sld/1.1.0/StyledLayerDescriptor.xsd">
  <NamedLayer>
    <se:Name>routier_emission_hexa</se:Name>
    <UserStyle>
      <se:Name>routier_emission_hexa</se:Name>

      <!-- BLOC 1 : Bordure grise (Casing) - Dessiné en premier pour tout le réseau -->
      <se:FeatureTypeStyle>
        <se:Rule>
          <se:Name>bordure_fond</se:Name>
          <se:LineSymbolizer uom="http://www.opengeospatial.org/se/units/metre">
            <se:Stroke>
              <se:SvgParameter name="stroke">#333333</se:SvgParameter>
              <se:SvgParameter name="stroke-width">7.5</se:SvgParameter>
              <se:SvgParameter name="stroke-linejoin">round</se:SvgParameter>
              <se:SvgParameter name="stroke-linecap">round</se:SvgParameter>
            </se:Stroke>
          </se:LineSymbolizer>
        </se:Rule>
      </se:FeatureTypeStyle>

      <!-- BLOC 2 : Remplissage couleur (Fill) - Dessiné par-dessus, génère la légende -->
      <se:FeatureTypeStyle>

        <!-- Moins de 70 dB -->
        <se:Rule>
          <se:Name>Moins de 70 dB</se:Name>
          <se:Description>
            <se:Title>&lt; 70 dB</se:Title>
          </se:Description>
          <ogc:Filter>
            <ogc:PropertyIsLessThan><ogc:PropertyName>hzd500</ogc:PropertyName><ogc:Literal>70</ogc:Literal></ogc:PropertyIsLessThan>
          </ogc:Filter>
          <se:LineSymbolizer uom="http://www.opengeospatial.org/se/units/metre">
            <se:Stroke>
              <se:SvgParameter name="stroke">#B8D6D1</se:SvgParameter>
              <se:SvgParameter name="stroke-width">5.5</se:SvgParameter>
              <se:SvgParameter name="stroke-linejoin">round</se:SvgParameter>
              <se:SvgParameter name="stroke-linecap">round</se:SvgParameter>
            </se:Stroke>
          </se:LineSymbolizer>
        </se:Rule>

        <!-- 70 - 75 dB -->
        <se:Rule>
          <se:Name>70 - 75 dB</se:Name>
          <se:Description>
            <se:Title>70 - 75 dB</se:Title>
          </se:Description>
          <ogc:Filter>
            <ogc:And>
              <ogc:PropertyIsGreaterThanOrEqualTo><ogc:PropertyName>hzd500</ogc:PropertyName><ogc:Literal>70</ogc:Literal></ogc:PropertyIsGreaterThanOrEqualTo>
              <ogc:PropertyIsLessThan><ogc:PropertyName>hzd500</ogc:PropertyName><ogc:Literal>75</ogc:Literal></ogc:PropertyIsLessThan>
            </ogc:And>
          </ogc:Filter>
          <se:LineSymbolizer uom="http://www.opengeospatial.org/se/units/metre">
            <se:Stroke>
              <se:SvgParameter name="stroke">#E2F2BF</se:SvgParameter>
              <se:SvgParameter name="stroke-width">5.5</se:SvgParameter>
              <se:SvgParameter name="stroke-linejoin">round</se:SvgParameter>
              <se:SvgParameter name="stroke-linecap">round</se:SvgParameter>
            </se:Stroke>
          </se:LineSymbolizer>
        </se:Rule>

        <!-- 75 - 78 dB -->
        <se:Rule>
          <se:Name>75 - 78 dB</se:Name>
          <se:Description>
            <se:Title>75 - 78 dB</se:Title>
          </se:Description>
          <ogc:Filter>
            <ogc:And>
              <ogc:PropertyIsGreaterThanOrEqualTo><ogc:PropertyName>hzd500</ogc:PropertyName><ogc:Literal>75</ogc:Literal></ogc:PropertyIsGreaterThanOrEqualTo>
              <ogc:PropertyIsLessThan><ogc:PropertyName>hzd500</ogc:PropertyName><ogc:Literal>78</ogc:Literal></ogc:PropertyIsLessThan>
            </ogc:And>
          </ogc:Filter>
          <se:LineSymbolizer uom="http://www.opengeospatial.org/se/units/metre">
            <se:Stroke>
              <se:SvgParameter name="stroke">#F3C683</se:SvgParameter>
              <se:SvgParameter name="stroke-width">5.5</se:SvgParameter>
              <se:SvgParameter name="stroke-linejoin">round</se:SvgParameter>
              <se:SvgParameter name="stroke-linecap">round</se:SvgParameter>
            </se:Stroke>
          </se:LineSymbolizer>
        </se:Rule>

        <!-- 78 - 81 dB -->
        <se:Rule>
          <se:Name>78 - 81 dB</se:Name>
          <se:Description>
            <se:Title>78 - 81 dB</se:Title>
          </se:Description>
          <ogc:Filter>
            <ogc:And>
              <ogc:PropertyIsGreaterThanOrEqualTo><ogc:PropertyName>hzd500</ogc:PropertyName><ogc:Literal>78</ogc:Literal></ogc:PropertyIsGreaterThanOrEqualTo>
              <ogc:PropertyIsLessThan><ogc:PropertyName>hzd500</ogc:PropertyName><ogc:Literal>81</ogc:Literal></ogc:PropertyIsLessThan>
            </ogc:And>
          </ogc:Filter>
          <se:LineSymbolizer uom="http://www.opengeospatial.org/se/units/metre">
            <se:Stroke>
              <se:SvgParameter name="stroke">#E87E4D</se:SvgParameter>
              <se:SvgParameter name="stroke-width">5.5</se:SvgParameter>
              <se:SvgParameter name="stroke-linejoin">round</se:SvgParameter>
              <se:SvgParameter name="stroke-linecap">round</se:SvgParameter>
            </se:Stroke>
          </se:LineSymbolizer>
        </se:Rule>

        <!-- 81 - 84 dB -->
        <se:Rule>
          <se:Name>81 - 84 dB</se:Name>
          <se:Description>
            <se:Title>81 - 84 dB</se:Title>
          </se:Description>
          <ogc:Filter>
            <ogc:And>
              <ogc:PropertyIsGreaterThanOrEqualTo><ogc:PropertyName>hzd500</ogc:PropertyName><ogc:Literal>81</ogc:Literal></ogc:PropertyIsGreaterThanOrEqualTo>
              <ogc:PropertyIsLessThan><ogc:PropertyName>hzd500</ogc:PropertyName><ogc:Literal>84</ogc:Literal></ogc:PropertyIsLessThan>
            </ogc:And>
          </ogc:Filter>
          <se:LineSymbolizer uom="http://www.opengeospatial.org/se/units/metre">
            <se:Stroke>
              <se:SvgParameter name="stroke">#CD463E</se:SvgParameter>
              <se:SvgParameter name="stroke-width">5.5</se:SvgParameter>
              <se:SvgParameter name="stroke-linejoin">round</se:SvgParameter>
              <se:SvgParameter name="stroke-linecap">round</se:SvgParameter>
            </se:Stroke>
          </se:LineSymbolizer>
        </se:Rule>

        <!-- 84 - 87 dB -->
        <se:Rule>
          <se:Name>84 - 87 dB</se:Name>
          <se:Description>
            <se:Title>84 - 87 dB</se:Title>
          </se:Description>
          <ogc:Filter>
            <ogc:And>
              <ogc:PropertyIsGreaterThanOrEqualTo><ogc:PropertyName>hzd500</ogc:PropertyName><ogc:Literal>84</ogc:Literal></ogc:PropertyIsGreaterThanOrEqualTo>
              <ogc:PropertyIsLessThan><ogc:PropertyName>hzd500</ogc:PropertyName><ogc:Literal>87</ogc:Literal></ogc:PropertyIsLessThan>
            </ogc:And>
          </ogc:Filter>
          <se:LineSymbolizer uom="http://www.opengeospatial.org/se/units/metre">
            <se:Stroke>
              <se:SvgParameter name="stroke">#A11A4D</se:SvgParameter>
              <se:SvgParameter name="stroke-width">5.5</se:SvgParameter>
              <se:SvgParameter name="stroke-linejoin">round</se:SvgParameter>
              <se:SvgParameter name="stroke-linecap">round</se:SvgParameter>
            </se:Stroke>
          </se:LineSymbolizer>
        </se:Rule>

        <!-- 87 - 92 dB -->
        <se:Rule>
          <se:Name>87 - 92 dB</se:Name>
          <se:Description>
            <se:Title>87 - 92 dB</se:Title>
          </se:Description>
          <ogc:Filter>
            <ogc:And>
              <ogc:PropertyIsGreaterThanOrEqualTo><ogc:PropertyName>hzd500</ogc:PropertyName><ogc:Literal>87</ogc:Literal></ogc:PropertyIsGreaterThanOrEqualTo>
              <ogc:PropertyIsLessThan><ogc:PropertyName>hzd500</ogc:PropertyName><ogc:Literal>92</ogc:Literal></ogc:PropertyIsLessThan>
            </ogc:And>
          </ogc:Filter>
          <se:LineSymbolizer uom="http://www.opengeospatial.org/se/units/metre">
            <se:Stroke>
              <se:SvgParameter name="stroke">#75085C</se:SvgParameter>
              <se:SvgParameter name="stroke-width">5.5</se:SvgParameter>
              <se:SvgParameter name="stroke-linejoin">round</se:SvgParameter>
              <se:SvgParameter name="stroke-linecap">round</se:SvgParameter>
            </se:Stroke>
          </se:LineSymbolizer>
        </se:Rule>

        <!-- Plus de 92 dB -->
        <se:Rule>
          <se:Name>Plus de 92 dB</se:Name>
          <se:Description>
            <se:Title>&gt; 92 dB</se:Title>
          </se:Description>
          <ogc:Filter>
            <ogc:PropertyIsGreaterThanOrEqualTo><ogc:PropertyName>hzd500</ogc:PropertyName><ogc:Literal>92</ogc:Literal></ogc:PropertyIsGreaterThanOrEqualTo>
          </ogc:Filter>
          <se:LineSymbolizer uom="http://www.opengeospatial.org/se/units/metre">
            <se:Stroke>
              <se:SvgParameter name="stroke">#430A4A</se:SvgParameter>
              <se:SvgParameter name="stroke-width">5.5</se:SvgParameter>
              <se:SvgParameter name="stroke-linejoin">round</se:SvgParameter>
              <se:SvgParameter name="stroke-linecap">round</se:SvgParameter>
            </se:Stroke>
          </se:LineSymbolizer>
        </se:Rule>

      </se:FeatureTypeStyle>
    </UserStyle>
  </NamedLayer>
</StyledLayerDescriptor>
