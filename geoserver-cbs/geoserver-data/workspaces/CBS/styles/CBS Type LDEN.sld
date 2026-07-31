<?xml version="1.0" encoding="UTF-8"?>
<StyledLayerDescriptor version="1.0.0" xsi:schemaLocation="http://www.opengis.net/sld StyledLayerDescriptor.xsd" xmlns="http://www.opengis.net/sld" xmlns:ogc="http://www.opengis.net/ogc" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <NamedLayer>
    <Name>CBS:cbs_hexa</Name>
    <UserStyle>
      <FeatureTypeStyle>
        
        <!-- Lden 55-59 -->
        <Rule>
          <Name>55 - 59 dB(A)</Name>
          <Title>55 - 59 dB(A)</Title>
          <ogc:Filter>
            <ogc:And>
              <ogc:PropertyIsEqualTo><ogc:PropertyName>cbstype</ogc:PropertyName><ogc:Literal>A</ogc:Literal></ogc:PropertyIsEqualTo>
              <ogc:PropertyIsEqualTo><ogc:PropertyName>indicetype</ogc:PropertyName><ogc:Literal>LD</ogc:Literal></ogc:PropertyIsEqualTo>
              <ogc:PropertyIsEqualTo><ogc:PropertyName>noiselevel</ogc:PropertyName><ogc:Literal>Lden5559</ogc:Literal></ogc:PropertyIsEqualTo>
            </ogc:And>
          </ogc:Filter>
          <PolygonSymbolizer>
            <Fill><CssParameter name="fill">#F3C683</CssParameter></Fill>
            <Stroke>
              <CssParameter name="stroke">#F3C683</CssParameter>
              <CssParameter name="stroke-width">0.5</CssParameter>
            </Stroke>
          </PolygonSymbolizer>
        </Rule>

        <!-- Lden 60-64 -->
        <Rule>
          <Name>60 - 64 dB(A)</Name>
          <Title>60 - 64 dB(A)</Title>
          <ogc:Filter>
            <ogc:And>
              <ogc:PropertyIsEqualTo><ogc:PropertyName>cbstype</ogc:PropertyName><ogc:Literal>A</ogc:Literal></ogc:PropertyIsEqualTo>
              <ogc:PropertyIsEqualTo><ogc:PropertyName>indicetype</ogc:PropertyName><ogc:Literal>LD</ogc:Literal></ogc:PropertyIsEqualTo>
              <ogc:PropertyIsEqualTo><ogc:PropertyName>noiselevel</ogc:PropertyName><ogc:Literal>Lden6064</ogc:Literal></ogc:PropertyIsEqualTo>
            </ogc:And>
          </ogc:Filter>
          <PolygonSymbolizer>
            <Fill><CssParameter name="fill">#E87E4D</CssParameter></Fill>
            <Stroke>
              <CssParameter name="stroke">#E87E4D</CssParameter>
              <CssParameter name="stroke-width">0.5</CssParameter>
            </Stroke>
          </PolygonSymbolizer>
        </Rule>

        <!-- Lden 65-69 -->
        <Rule>
          <Name>65 - 69 dB(A)</Name>
          <Title>65 - 69 dB(A)</Title>
          <ogc:Filter>
            <ogc:And>
              <ogc:PropertyIsEqualTo><ogc:PropertyName>cbstype</ogc:PropertyName><ogc:Literal>A</ogc:Literal></ogc:PropertyIsEqualTo>
              <ogc:PropertyIsEqualTo><ogc:PropertyName>indicetype</ogc:PropertyName><ogc:Literal>LD</ogc:Literal></ogc:PropertyIsEqualTo>
              <ogc:PropertyIsEqualTo><ogc:PropertyName>noiselevel</ogc:PropertyName><ogc:Literal>Lden6569</ogc:Literal></ogc:PropertyIsEqualTo>
            </ogc:And>
          </ogc:Filter>
          <PolygonSymbolizer>
            <Fill><CssParameter name="fill">#CD463E</CssParameter></Fill>
            <Stroke>
              <CssParameter name="stroke">#CD463E</CssParameter>
              <CssParameter name="stroke-width">0.5</CssParameter>
            </Stroke>
          </PolygonSymbolizer>
        </Rule>

        <!-- Lden 70-74 -->
        <Rule>
          <Name>70 - 74 dB(A)</Name>
          <Title>70 - 74 dB(A)</Title>
          <ogc:Filter>
            <ogc:And>
              <ogc:PropertyIsEqualTo><ogc:PropertyName>cbstype</ogc:PropertyName><ogc:Literal>A</ogc:Literal></ogc:PropertyIsEqualTo>
              <ogc:PropertyIsEqualTo><ogc:PropertyName>indicetype</ogc:PropertyName><ogc:Literal>LD</ogc:Literal></ogc:PropertyIsEqualTo>
              <ogc:PropertyIsEqualTo><ogc:PropertyName>noiselevel</ogc:PropertyName><ogc:Literal>Lden7074</ogc:Literal></ogc:PropertyIsEqualTo>
            </ogc:And>
          </ogc:Filter>
          <PolygonSymbolizer>
            <Fill><CssParameter name="fill">#A11A4D</CssParameter></Fill>
            <Stroke>
              <CssParameter name="stroke">#A11A4D</CssParameter>
              <CssParameter name="stroke-width">0.5</CssParameter>
            </Stroke>
          </PolygonSymbolizer>
        </Rule>

        <!-- Lden > 75 -->
        <Rule>
          <Name>&gt; 75 dB(A)</Name>
          <Title>&gt; 75 dB(A)</Title>
          <ogc:Filter>
            <ogc:And>
              <ogc:PropertyIsEqualTo><ogc:PropertyName>cbstype</ogc:PropertyName><ogc:Literal>A</ogc:Literal></ogc:PropertyIsEqualTo>
              <ogc:PropertyIsEqualTo><ogc:PropertyName>indicetype</ogc:PropertyName><ogc:Literal>LD</ogc:Literal></ogc:PropertyIsEqualTo>
              <ogc:PropertyIsEqualTo><ogc:PropertyName>noiselevel</ogc:PropertyName><ogc:Literal>LdenGreaterThan75</ogc:Literal></ogc:PropertyIsEqualTo>
            </ogc:And>
          </ogc:Filter>
          <PolygonSymbolizer>
            <Fill><CssParameter name="fill">#75085C</CssParameter></Fill>
            <Stroke>
              <CssParameter name="stroke">#75085C</CssParameter>
              <CssParameter name="stroke-width">0.5</CssParameter>
            </Stroke>
          </PolygonSymbolizer>
        </Rule>

      </FeatureTypeStyle>
    </UserStyle>
  </NamedLayer>
</StyledLayerDescriptor>