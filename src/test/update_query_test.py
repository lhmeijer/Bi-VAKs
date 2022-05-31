import unittest
from src.main.bitr4qs.webservice.app import create_app
from src.main.bitr4qs.configuration import get_default_configuration


class UpdateQueryTest(unittest.TestCase):

    def test_insert_multiple_triples_explicit(self):
        args = get_default_configuration()
        args['referenceStrategy'] = {'explicit': True, 'implicit': False}
        app = create_app(args).test_client()
        update = """
        PREFIX :  <http://recipehub.nl/recipes#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        INSERT DATA {
          :RecipeRicottaDoughnuts a :Recipe .
          :RecipeRicottaDoughnuts :cuisine :EnglishCuisine .
          :RecipeRicottaDoughnuts :ingredients :Flour .
          :RecipeRicottaDoughnuts :ingredients :CasterSugar .
          :RicottaCheese a :Food .
          :RicottaCheese rdfs:label "Ricotta cheese"@en-gb .
          :RicottaCheese rdfs:label "Ricotta"@nl .
          :RecipeRicottaDoughnuts :ingredients :RicottaCheese .
          :RecipeRicottaDoughnuts :ingredients :Egg .
          :RecipeRicottaDoughnuts :meal :Snack .
          :RecipeRicottaDoughnuts :produces :RicottaDoughnut .
          :RicottaDoughnut a :Food .
          :RicottaDoughnut rdfs:label "Ricotta doughnut"@en-gb .
          :RicottaDoughnut rdfs:label "Ricotta doughnut"@nl .
          :RecipeRicottaDoughnuts rdfs:comment "Zeppole are irresistible light ricotta doughnuts that are a snip to make at home."@en-gb .
          :RecipeRicottaDoughnuts rdfs:label "Ricotta doughnuts"@en-gb .
        }
        """
        response = app.post('/update', data=dict(update=update, author='Tom de Vries', test='shouldBeTested',
                                                 startDate="2021-06-20T00:00:00+00:00",
                                                 endDate="2021-08-25T00:00:00+00:00",
                                                 description='Add recipe of ricotta doughnuts.'))
        self.assertEqual(response.status_code, 200)

    def test_delete_multiple_triples_explicit(self):
        args = get_default_configuration()
        args['referenceStrategy'] = {'explicit': True, 'implicit': False}
        app = create_app(args).test_client()
        update = """
        PREFIX :  <http://recipehub.nl/recipes#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        DELETE DATA {
          :RecipeChocolateFudgeBrownies a :Recipe .
          :RecipeChocolateFudgeBrownies :cuisine :EnglishCuisine .
          :RecipeChocolateFudgeBrownies :ingredients :CasterSugar .
          :RecipeChocolateFudgeBrownies :ingredients :Butter .
          :RecipeChocolateFudgeBrownies :ingredients :SelfRaisingFlour .
          :RecipeChocolateFudgeBrownies :meal :Snack .
          :RecipeChocolateFudgeBrownies :produces :ChocolateFudgeBrownie .
          :RecipeChocolateFudgeBrownies rdfs:comment "A genuine brownie should first and foremost taste of chocolate. There should be undertones of coffee and vanilla and it should be dark and nutty, with a fudge-like centre and a firm, slightly crispy outer surface."@en-gb .
          :RecipeChocolateFudgeBrownies rdfs:label "Chocolate fudge brownies"@en-gb .
        }
        """
        response = app.post('/update', data=dict(update=update, author='Peter Schouten', test='yes',
                                                 startDate="2021-06-24T00:00:00+00:00",
                                                 endDate="2021-07-06T00:00:00+00:00",
                                                 description='Delete recipe of chocolate fudge brownies.'))
        self.assertEqual(response.status_code, 200)

    def test_insert_and_delete_multiple_quads_explicit(self):
        args = get_default_configuration()
        args['referenceStrategy'] = {'explicit': True, 'implicit': False}
        app = create_app(args).test_client()
        update = """
        PREFIX :  <http://recipehub.nl/recipes#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        DELETE DATA {
            GRAPH :VegetableBible {
                :RecipeCourgetteGratin a :Recipe .
                :RecipeCourgetteGratin :cuisine :FrenchCuisine .
                :RecipeCourgetteGratin :ingredients :Courgette .
                :RecipeCourgetteGratin rdfs:comment "This simple courgette gratin makes a great side dish as it’s creamy and flavoursome without being too rich. Serve with a salad for a light supper."@en-gb .
                :RecipeCourgetteGratin :meal :SideDish .
                :RecipeCourgetteGratin :diet :Vegetarian .
                :RecipeCourgetteGratin :produces :CourgetteGratin .
                :RecipeCourgetteGratin rdfs:label "Courgette gratin"@en-gb .
            }
        } ;
        INSERT DATA {
            GRAPH :GreatCurries {
                :RecipeKeemaLambCurry a :Recipe .
                :RecipeKeemaLambCurry :cuisine :IndianCuisine .
                :RecipeKeemaLambCurry :ingredients :Onion .
                :RecipeKeemaLambCurry :ingredients :Garlic .
                :RecipeKeemaLambCurry :ingredients :Ginger .
                :RecipeKeemaLambCurry :ingredients :MincedLamb .
                :MincedLamb a :Food .
                :MincedLamb rdfs:label "Minced lamb"@en-gb .
                :MincedLamb rdfs:label "Lamsgehakt"@nl .
                :RecipeKeemaLambCurry :meal :MainCourse .
                :RecipeKeemaLambCurry :produces :KeemaLambCurry .
                :KeemaLambCurry a :Food .
                :KeemaLambCurry rdfs:label "Keema lamb curry"@en-gb .
                :KeemaLambCurry rdfs:label "Keema lams curry"@nl .
                :KeemaLambCurry rdfs:comment "A great family dinner; a bit like mince and tatties with a kick. If your family prefers a milder curry, reduce the chilli powder to a pinch."@en-gb .
                :KeemaLambCurry rdfs:label "Keema lamb curry"@en-gb .
            }
            GRAPH :VegetableBible {
                :RecipeBroccoliWithYoghurt a :Recipe .
                :RecipeBroccoliWithYoghurt :ingredients :Broccoli .
                :RecipeBroccoliWithYoghurt :ingredients :Yoghurt .
                :RecipeBroccoliWithYoghurt :ingredients :SpringOnion .
                :RecipeBroccoliWithYoghurt :meal :SideDish .
                :RecipeBroccoliWithYoghurt :produces :BroccoliWithYoghurt .
                :BroccoliWithYoghurt a :Food .
                :BroccoliWithYoghurt rdfs:label "Broccoli with yoghurt"@en-gb .
                :BroccoliWithYoghurt rdfs:label "Broccoli met yoghurt"@nl .
                :RecipeBroccoliWithYoghurt rdfs:label "Broccoli with yoghurt"@en-gb .
            }
        }

        """
        response = app.post('/update', data=dict(update=update, author='Marjolein de Jong', test='yes',
                                                 startDate="2021-07-01T00:00:00+00:00",
                                                 endDate="2021-08-30T00:00:00+00:00",
                                                 description='Delete recipe of courgette gratin, and add recipes of '
                                                             'keema lamb curry and of broccoli with yoghurt.'))
        self.assertEqual(response.status_code, 200)

    def test_insert_triple_exist_explicit(self):
        args = get_default_configuration()
        args['referenceStrategy'] = {'explicit': True, 'implicit': False}
        app = create_app(args).test_client()
        update = """
        PREFIX :  <http://recipehub.nl/recipes#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        INSERT DATA {
          :RecipeMexicanCoffeeBuns a :Recipe .
          :RecipeMexicanCoffeeBuns :cuisine :MexicanCuisine .
          :RecipeMexicanCoffeeBuns :ingredients :Flour .
          :RecipeMexicanCoffeeBuns :ingredients :Butter .
          :RecipeMexicanCoffeeBuns :ingredients :Yeast .
          :RecipeMexicanCoffeeBuns :ingredients :Milk .
          :RecipeMexicanCoffeeBuns :ingredients :Egg .
          :RecipeMexicanCoffeeBuns :meal :Lunch .
          :RecipeMexicanCoffeeBuns :produces :MexicanCoffeeBun .
          :RecipeMexicanCoffeeBuns rdfs:comment "These buns are sold all over Malaysia and a bakery chain called Rotiboy has made them so famous that they are often called Rotiboy buns."@en-gb .
          :RecipeMexicanCoffeeBuns rdfs:label "Mexican coffee buns"@en-gb .
        }
        """
        response = app.post('/update', data=dict(update=update, author='Lisa Meijer', test='yes',
                                                 startDate="2021-06-20T00:00:00+00:00",
                                                 endDate="2021-08-25T00:00:00+00:00",
                                                 description='Add recipe of mexican coffee buns.'))
        self.assertEqual(response.status_code, 200)

    def test_delete_triple_not_exist_explicit(self):
        args = get_default_configuration()
        args['referenceStrategy'] = {'explicit': True, 'implicit': False}
        app = create_app(args).test_client()
        update = """
        PREFIX :  <http://recipehub.nl/recipes#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        DELETE DATA {
          :RecipeMexicanCoffeeBuns a :Recipe .
          :RecipeMexicanCoffeeBuns :cuisine :MexicanCuisine .
          :RecipeMexicanCoffeeBuns :ingredients :Flour .
          :RecipeMexicanCoffeeBuns :ingredients :Butter .
          :RecipeMexicanCoffeeBuns :ingredients :Yeast .
          :RecipeMexicanCoffeeBuns :ingredients :Milk .
          :RecipeMexicanCoffeeBuns :ingredients :Egg .
          :RecipeMexicanCoffeeBuns :meal :Lunch .
          :RecipeMexicanCoffeeBuns :produces :MexicanCoffeeBun .
          :RecipeMexicanCoffeeBuns rdfs:comment "These buns are sold all over Malaysia and a bakery chain called Rotiboy has made them so famous that they are often called Rotiboy buns."@en-gb .
          :RecipeMexicanCoffeeBuns rdfs:label "Mexican coffee buns"@en-gb .
        }
        """
        response = app.post('/update', data=dict(update=update, author='Ivo Mulder', test='yes',
                                                 startDate="2021-08-09T00:00:00+00:00",
                                                 endDate="2021-08-25T00:00:00+00:00",
                                                 description='Delete recipe of mexican coffee buns.'))
        self.assertEqual(response.status_code, 200)

    def test_insert_delete_insert_delete_triples_explicit(self):
        args = get_default_configuration()
        args['referenceStrategy'] = {'explicit': True, 'implicit': False}
        app = create_app(args).test_client()
        updateInsert = """
        PREFIX :  <http://recipehub.nl/recipes#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        INSERT DATA {
            :DutchCuisine a :Cuisine .
            :DutchCuisine rdfs:label "Nederlands"@nl .
            :DutchCuisine rdfs:label "Dutch"@en-gb .
        }
        """
        updateDelete = """
        PREFIX :  <http://recipehub.nl/recipes#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        DELETE DATA {
            :DutchCuisine a :Cuisine .
            :DutchCuisine rdfs:label "Nederlands"@nl .
            :DutchCuisine rdfs:label "Dutch"@en-gb .
        }
        """
        response = app.post('/update', data=dict(update=updateInsert, author='Reinier van Beek',
                                                 startDate="2021-06-16T00:00:00+00:00",
                                                 endDate="2021-12-30T00:00:00+00:00", description='Add dutch cuisine.'))

        response = app.post('/update', data=dict(update=updateDelete, author='Reinier van Beek',
                                                 startDate="2021-08-10T00:00:00+00:00",
                                                 endDate="2021-11-17T00:00:00+00:00",
                                                 description='Delete dutch cuisine.'))

        response = app.post('/update', data=dict(update=updateInsert, author='Reinier van Beek',
                                                 startDate="2021-09-09T00:00:00+00:00",
                                                 endDate="2021-11-02T00:00:00+00:00", description='Add dutch cuisine.'))

        response = app.post('/update', data=dict(update=updateDelete, author='Reinier van Beek',
                                                 startDate="2021-09-22T00:00:00+00:00",
                                                 endDate="2021-10-16T00:00:00+00:00",
                                                 description='Delete dutch cuisine.'))

    def test_insert_and_delete_triples_to_branch_explicit(self):
        args = get_default_configuration()
        args['referenceStrategy'] = {'explicit': True, 'implicit': False}
        app = create_app(args).test_client()
        update = """
        PREFIX :  <http://recipehub.nl/recipes#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        DELETE DATA {
            :Cashewnut a :Food .
            :Cashewnut rdfs:label "Cashewnoot"@nl .
            :Cashewnut rdfs:label "Cashewnut"@en-gb .
        };
        INSERT DATA {
            :Strawberry a :Food .
            :Strawberry rdfs:label "Aardbei"@nl .
            :Strawberry rdfs:label "Strawberry"@en-gb .
        }
        """
        response = app.post('/update', data=dict(update=update, author='Reinier van Beek', branch='SweetRecipes',
                                                 startDate="2021-07-01T00:00:00+00:00",
                                                 endDate="2021-07-30T00:00:00+00:00",
                                                 description='Add strawberry and delete cashew nut .'))

    def test_insert_multiple_triples_implicit(self):
        args = get_default_configuration()
        args['referenceStrategy'] = {'explicit': False, 'implicit': True}
        app = create_app(args).test_client()
        update = """
        PREFIX :  <http://recipehub.nl/recipes#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        INSERT DATA {
          :RecipeRicottaDoughnuts a :Recipe .
          :RecipeRicottaDoughnuts :cuisine :EnglishCuisine .
          :RecipeRicottaDoughnuts :ingredients :Flour .
          :RecipeRicottaDoughnuts :ingredients :CasterSugar .
          :RicottaCheese a :Food .
          :RicottaCheese rdfs:label "Ricotta cheese"@en-gb .
          :RicottaCheese rdfs:label "Ricotta"@nl .
          :RecipeRicottaDoughnuts :ingredients :RicottaCheese .
          :RecipeRicottaDoughnuts :ingredients :Egg .
          :RecipeRicottaDoughnuts :meal :Snack .
          :RecipeRicottaDoughnuts :produces :RicottaDoughnut .
          :RicottaDoughnut a :Food .
          :RicottaDoughnut rdfs:label "Ricotta doughnut"@en-gb .
          :RicottaDoughnut rdfs:label "Ricotta doughnut"@nl .
          :RecipeRicottaDoughnuts rdfs:comment "Zeppole are irresistible light ricotta doughnuts that are a snip to make at home."@en-gb .
          :RecipeRicottaDoughnuts rdfs:label "Ricotta doughnuts"@en-gb .
        }
        """
        response = app.post('/update', data=dict(update=update, author='Tom de Vries',
                                                 startDate="2021-06-20T00:00:00+00:00",
                                                 endDate="2021-08-25T00:00:00+00:00",
                                                 description='Add recipe of ricotta doughnuts.'))
        self.assertEqual(response.status_code, 200)

    def test_delete_multiple_quads_implicit(self):
        args = get_default_configuration()
        args['referenceStrategy'] = {'explicit': False, 'implicit': True}
        app = create_app(args).test_client()
        update = """
        PREFIX :  <http://recipehub.nl/recipes#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        DELETE DATA {
          :RecipeChocolateFudgeBrownies a :Recipe .
          :RecipeChocolateFudgeBrownies :cuisine :EnglishCuisine .
          :RecipeChocolateFudgeBrownies :ingredients :CasterSugar .
          :RecipeChocolateFudgeBrownies :ingredients :Butter .
          :RecipeChocolateFudgeBrownies :ingredients :SelfRaisingFlour .
          :RecipeChocolateFudgeBrownies :meal :Snack .
          :RecipeChocolateFudgeBrownies :produces :ChocolateFudgeBrownie .
          :RecipeChocolateFudgeBrownies rdfs:comment "A genuine brownie should first and foremost taste of chocolate. There should be undertones of coffee and vanilla and it should be dark and nutty, with a fudge-like centre and a firm, slightly crispy outer surface."@en-gb .
          :RecipeChocolateFudgeBrownies rdfs:label "Chocolate fudge brownies"@en-gb .
        }
        """
        response = app.post('/update', data=dict(update=update, author='Peter Schouten',
                                                 startDate="2021-06-24T00:00:00+00:00",
                                                 endDate="2021-07-06T00:00:00+00:00",
                                                 description='Delete recipe of chocolate fudge brownies.'))
        self.assertEqual(response.status_code, 200)

    def test_insert_and_delete_multiple_triples_implicit(self):
        pass

    def test_insert_triple_exist_implicit(self):
        pass

    def test_delete_triple_not_exist_implicit(self):
        pass

    def test_insert_and_delete_triples_to_branch_implicit(self):
        args = get_default_configuration()
        args['referenceStrategy'] = {'explicit': True, 'implicit': False}
        app = create_app(args).test_client()
        update = """
        PREFIX :  <http://recipehub.nl/recipes#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        DELETE DATA {
            :Cashewnut a :Food .
            :Cashewnut rdfs:label "Cashewnoot"@nl .
            :Cashewnut rdfs:label "Cashewnut"@en-gb .
        };
        INSERT DATA {
            :Strawberry a :Food .
            :Strawberry rdfs:label "Aardbei"@nl .
            :Strawberry rdfs:label "Strawberry"@en-gb .
        }
        """
        response = app.post('/update', data=dict(update=update, author='Reinier van Beek', branch='SweetRecipes',
                                                 startDate="2021-07-01T00:00:00+00:00",
                                                 endDate="2021-07-30T00:00:00+00:00",
                                                 description='Add strawberry and delete cashew nut .'))
